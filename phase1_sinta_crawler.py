"""
Fase 1 — Crawl daftar jurnal Sinta 3 & 4 dari portal sinta.kemdiktisaintek.go.id

Menggunakan requests + BeautifulSoup (portal server-side rendered, tidak butuh Playwright).
Filter dikirim via POST form dengan session cookie, lalu paginate via GET.

Output: data/journals_raw.csv
Format: nama_jurnal,url,sinta_grade,subject_area
"""

import csv
import time
import re
import asyncio
import requests
from bs4 import BeautifulSoup

from config import (
    SINTA_BASE_URL,
    SINTA_GRADES,
    PATHS,
    REQUEST_DELAY_MIN,
    REQUEST_DELAY_MAX,
    MAX_RETRIES,
    USER_AGENT,
    get_sinta_subject_labels,
    SINTA_SUBJECT_MAP,
)
from utils.logger import get_logger

logger = get_logger("phase1")

# Mapping label subject → nilai filter_area di form
_SUBJECT_AREA_VALUE: dict[str, str] = {
    "Agriculture": "7",
    "Art":         "8",
    "Economy":     "2",
    "Education":   "6",
    "Engineering": "10",
    "Health":      "4",
    "Humanities":  "3",
    "Religion":    "1",
    "Science":     "5",
    "Social":      "9",
}

_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def _build_filter_data() -> dict:
    """
    Bangun data POST untuk filter form Sinta.
    Sertakan grade (filter_accreditation) dan subject (filter_area) yang aktif.
    """
    data: dict[str, str] = {}

    # Rank filter
    for grade in SINTA_GRADES:
        data[f"filter_accreditation[{grade}]"] = str(grade)

    # Subject filter
    for label in get_sinta_subject_labels():
        val = _SUBJECT_AREA_VALUE.get(label)
        if val:
            data[f"filter_area[{val}]"] = val

    # Submit button
    data["filter_journals"] = "1"

    return data


def _get_total_pages(soup: BeautifulSoup) -> int:
    """Ambil total halaman dari pagination atau teks 'Page X of Y'."""
    # Cek teks "Page X of Y | Total Records N"
    page_info = re.search(r"Page\s+\d+\s+of\s+([\d,\.]+)", soup.get_text(), re.I)
    if page_info:
        return int(page_info.group(1).replace(",", "").replace(".", ""))

    # Fallback: ambil angka terbesar dari pagination link
    nums = []
    for a in soup.select(".pagination a[href*='page=']"):
        m = re.search(r"page=(\d+)", a.get("href", ""))
        if m:
            nums.append(int(m.group(1)))
    return max(nums) if nums else 1


def _parse_journals(soup: BeautifulSoup) -> list[dict]:
    """
    Parse daftar jurnal dari satu halaman HTML Sinta.
    Struktur HTML:
      div.list-item.row.mt-3
        div.col-lg.meta-side
          div.affil-name   → nama + link profil Sinta
          div.affil-abbrev → link Website, Google Scholar, Editor URL
          div.profile-id   → ISSN + Subject Area
          div.stat-prev    → badge S3/S4 Accredited
    """
    journals = []

    # Setiap jurnal ada dalam div.list-item
    blocks = soup.select("div.list-item.row")
    if not blocks:
        # Fallback: ambil semua link profil jurnal
        blocks = [a.find_parent("div", class_=re.compile(r"col-lg|meta-side|media"))
                  for a in soup.select("a[href*='/journals/profile/']")]
        blocks = [b for b in blocks if b]

    for block in blocks:
        # ── Nama jurnal ──────────────────────────────────────────────────────
        name_el = block.select_one("div.affil-name a[href*='/journals/profile/']")
        if not name_el:
            continue
        name = name_el.get_text(strip=True)
        if not name or len(name) < 4:
            continue

        # ── URL jurnal asli dari div.affil-abbrev → link "Website" ──────────
        journal_url = ""
        abbrev = block.select_one("div.affil-abbrev")
        if abbrev:
            for a in abbrev.select("a[href^='http']"):
                href = a.get("href", "")
                if (
                    "sinta.kemdi" not in href
                    and "scholar.google" not in href
                    and "garuda.kemdi" not in href
                    and "scopus.com" not in href
                    and href != "#!"
                ):
                    journal_url = href.rstrip("/")
                    break

        if not journal_url:
            logger.debug(f"  skip (no URL): {name}")
            continue

        # ── Subject area dari div.profile-id ─────────────────────────────────
        subject_area = ""
        profile_id = block.select_one("div.profile-id")
        if profile_id:
            text = profile_id.get_text(" ", strip=True)
            m = re.search(r"Subject\s*Area\s*[:\-]?\s*([\w\s,]+)", text, re.I)
            if m:
                subject_area = m.group(1).strip()

        # ── Grade dari div.stat-prev → span.num-stat.accredited ──────────────
        grade = 3  # default Sinta 3
        stat_prev = block.select_one("div.stat-prev")
        if stat_prev:
            accred_text = stat_prev.get_text(" ", strip=True)
            gm = re.search(r"\bS([1-6])\b", accred_text)
            if gm:
                grade = int(gm.group(1))

        # Hanya simpan Sinta 3 dan 4
        if grade not in SINTA_GRADES:
            continue

        journals.append({
            "nama_jurnal":  name,
            "url":          journal_url,
            "sinta_grade":  grade,
            "subject_area": subject_area,
        })

    return journals


def _fetch_with_retry(session: requests.Session, method: str, url: str,
                      data: dict | None = None, retries: int = MAX_RETRIES) -> requests.Response | None:
    """GET atau POST dengan retry otomatis."""
    for attempt in range(1, retries + 1):
        try:
            if method == "POST":
                r = session.post(url, data=data, timeout=20)
            else:
                r = session.get(url, timeout=20)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            logger.warning(f"  Attempt {attempt}/{retries} gagal: {e}")
            if attempt < retries:
                time.sleep(REQUEST_DELAY_MIN * attempt)
    return None


async def run_phase1(subjects: list[str] | None = None):
    """
    Entry point Fase 1: crawl jurnal Sinta dengan filter rank + subject via POST form.
    Menggunakan requests (bukan Playwright) karena portal server-side rendered.
    """
    if subjects is not None:
        from config import set_subjects
        set_subjects(subjects)

    from config import PATHS
    paths      = PATHS
    all_journals: list[dict] = []
    seen_urls: set[str] = set()

    subject_labels = get_sinta_subject_labels()
    filter_data    = _build_filter_data()

    logger.info(f"=== Mulai crawl Sinta (requests + POST filter) ===")
    logger.info(f"Filter → Rank: {SINTA_GRADES} | Subject: {subject_labels}")
    logger.info(f"POST data: {filter_data}")

    session = requests.Session()
    session.headers.update(_HEADERS)

    # ── Langkah 1: GET halaman awal untuk inisialisasi session cookie ──────────
    r0 = _fetch_with_retry(session, "GET", SINTA_BASE_URL)
    if not r0:
        logger.error("Gagal akses portal Sinta")
        return []
    logger.info(f"Session cookie: {dict(session.cookies)}")

    # ── Langkah 2: POST filter untuk set filter di server-side session ─────────
    r1 = _fetch_with_retry(session, "POST", SINTA_BASE_URL, data=filter_data)
    if not r1:
        logger.error("Gagal POST filter ke portal Sinta")
        return []

    soup1    = BeautifulSoup(r1.text, "lxml")
    total_pages = _get_total_pages(soup1)

    # Hitung total records
    total_match = re.search(r"Total Records?\s+([\d,\.]+)", r1.text, re.I)
    total_records = total_match.group(1) if total_match else "?"
    logger.info(f"Total jurnal setelah filter: {total_records} | Total halaman: {total_pages}")

    # Parse halaman 1
    page1_journals = _parse_journals(soup1)
    for j in page1_journals:
        if j["url"] not in seen_urls:
            seen_urls.add(j["url"])
            all_journals.append(j)
    logger.info(f"Halaman 1/{total_pages}: {len(page1_journals)} jurnal (valid: {len(all_journals)})")

    # ── Langkah 3: GET halaman 2..N dengan session yang sudah punya filter ─────
    for page_num in range(2, total_pages + 1):
        url = f"{SINTA_BASE_URL}?page={page_num}"
        r = _fetch_with_retry(session, "GET", url)
        if not r:
            logger.warning(f"Halaman {page_num} gagal, skip")
            continue

        soup    = BeautifulSoup(r.text, "lxml")
        journals = _parse_journals(soup)
        added   = 0
        for j in journals:
            if j["url"] not in seen_urls:
                seen_urls.add(j["url"])
                all_journals.append(j)
                added += 1

        logger.info(f"Halaman {page_num}/{total_pages}: {len(journals)} jurnal (baru: {added}, total: {len(all_journals)})")

        # Jeda antar halaman
        time.sleep(REQUEST_DELAY_MIN)

    logger.info(f"=== Crawl selesai: {len(all_journals)} jurnal ===")

    # ── Tulis ke journals_raw.csv ──────────────────────────────────────────────
    fieldnames = ["nama_jurnal", "url", "sinta_grade", "subject_area"]
    with open(paths["journals_raw_csv"], "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_journals)

    logger.info(f"Phase 1 selesai. {len(all_journals)} jurnal → {paths['journals_raw_csv']}")
    return all_journals


if __name__ == "__main__":
    asyncio.run(run_phase1())
