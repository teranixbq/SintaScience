"""
Fase 3 — OAI-PMH Harvester
Input:  {run_dir}/journals_publisher_oai_active.txt
Output: {run_dir}/articles.csv + {run_dir}/progress.json

Edge case yang di-handle:
  1. HTTP 429/503 rate limiting → backoff + retry
  2. XML encoding rusak (Latin-1/mixed) → fallback repair
  3. resumptionToken expired (badResumptionToken) → restart dari awal
  4. OAI error noRecordsMatch → selesai normal, bukan error
  5. Thread-safe progress via _progress_lock
  6. Batch tulis CSV incremental, tidak tunggu selesai semua
"""

import asyncio
import json
import threading
import time
import random
from pathlib import Path
from urllib.parse import urlencode, quote

import requests
from lxml import etree
from tenacity import (
    retry, stop_after_attempt, wait_exponential,
    retry_if_exception_type, retry_if_result,
    before_sleep_log, RetryError,
)
import logging

from config import (
    PATHS,
    REQUEST_TIMEOUT,
    REQUEST_DELAY_MIN,
    REQUEST_DELAY_MAX,
    MAX_RETRIES,
    MAX_CONCURRENT_JOURNALS,
    USER_AGENT,
    CSV_BATCH_SIZE,
    HARVEST_YEAR_FROM,
    HARVEST_YEAR_UNTIL,
    OAI_METADATA_PREFIX,
)
from utils.logger import get_logger
from utils.csv_writer import init_articles_csv, write_articles_batch

logger = get_logger("phase3")

NS = {
    "oai":    "http://www.openarchives.org/OAI/2.0/",
    "dc":     "http://purl.org/dc/elements/1.1/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
}

HEADERS = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"}
_progress_lock = threading.Lock()


# ── Progress helpers ───────────────────────────────────────────────────────────

def load_progress(progress_json: Path) -> dict:
    if progress_json.exists():
        try:
            with open(progress_json, encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            logger.warning("progress.json rusak, mulai dari awal")
    return {}


def update_progress(progress: dict, key: str, state: dict, progress_json: Path):
    """Thread-safe update satu entry progress dan simpan ke file."""
    with _progress_lock:
        progress[key] = state
        tmp = progress_json.with_suffix(".tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(progress, f, indent=2, ensure_ascii=False)
            tmp.replace(progress_json)   # atomic rename — hindari file korup kalau crash
        except OSError as e:
            logger.error(f"Gagal simpan progress: {e}")


# ── HTTP helper ────────────────────────────────────────────────────────────────

def _is_rate_limited(resp: requests.Response) -> bool:
    return resp.status_code in (429, 503)


def _get_oai_raw(session: requests.Session, url: str) -> requests.Response:
    """
    GET OAI endpoint dengan handling:
    - Timeout / ConnectionError → retry via tenacity
    - HTTP 429/503 rate limit → backoff + retry
    - HTTP 4xx/5xx lain → raise HTTPError
    """
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT, headers=HEADERS, allow_redirects=True)
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt < MAX_RETRIES - 1:
                wait = 2 ** (attempt + 1) + random.uniform(0, 2)
                logger.warning(f"  Connection error (attempt {attempt+1}/{MAX_RETRIES}), retry in {wait:.1f}s: {e}")
                time.sleep(wait)
                continue
            raise

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            logger.warning(f"  HTTP 429 Too Many Requests, tunggu {retry_after}s...")
            time.sleep(retry_after + random.uniform(1, 5))
            continue

        if resp.status_code == 503:
            wait = 30 + random.uniform(0, 15)
            logger.warning(f"  HTTP 503 Service Unavailable, tunggu {wait:.0f}s...")
            time.sleep(wait)
            continue

        resp.raise_for_status()
        return resp

    raise requests.RetryError(f"Gagal setelah {MAX_RETRIES} attempts: {url}")


# ── XML encoding repair ────────────────────────────────────────────────────────

def _safe_parse_xml(xml_bytes: bytes):
    """
    Parse XML dengan fallback encoding repair.
    Beberapa OJS Indonesia encode konten sebagai Latin-1 tapi deklarasi XML-nya UTF-8,
    menyebabkan etree.fromstring() gagal dengan XMLSyntaxError.

    Strategi:
    1. Coba parse langsung (paling cepat, handle mayoritas kasus)
    2. Kalau gagal, coba decode sebagai latin-1 lalu encode ulang ke UTF-8
    3. Kalau masih gagal, gunakan recover=True (lxml toleran terhadap malformed XML)
    """
    # Attempt 1: parse langsung
    try:
        return etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError:
        pass

    # Attempt 2: repair encoding declaration
    try:
        text = xml_bytes.decode("latin-1", errors="replace")
        # Ganti deklarasi encoding jika ada
        text = text.replace('encoding="UTF-8"', 'encoding="latin-1"')
        text = text.replace("encoding='UTF-8'", "encoding='latin-1'")
        return etree.fromstring(text.encode("latin-1"))
    except Exception:
        pass

    # Attempt 3: lxml recover mode (parse meski ada error)
    try:
        parser = etree.XMLParser(recover=True, encoding="utf-8")
        root   = etree.fromstring(xml_bytes, parser=parser)
        if root is not None:
            logger.warning("XML parsed dengan recover mode — data mungkin tidak lengkap")
            return root
    except Exception:
        pass

    return None


# ── URL builder ────────────────────────────────────────────────────────────────

def build_list_records_url(oai_url: str, resumption_token: str = None) -> str:
    """
    Build URL OAI-PMH ListRecords.
    Saat pakai resumptionToken, parameter lain TIDAK boleh ada (spec OAI-PMH 2.0).
    Filter tahun dilakukan di parse_records() bukan di parameter OAI karena
    from/until OAI merujuk ke datestamp record, bukan pub_date artikel.
    """
    base = oai_url.rstrip("/")
    if resumption_token:
        return f"{base}?verb=ListRecords&resumptionToken={quote(resumption_token, safe='')}"
    return f"{base}?{urlencode({'verb': 'ListRecords', 'metadataPrefix': OAI_METADATA_PREFIX})}"


# ── XML helpers (top-level, bukan nested dalam loop) ──────────────────────────

def _get_all_dc(dc_el, tag: str) -> list[str]:
    """Ambil semua nilai dc:tag dari elemen dc. Defined di luar loop untuk efisiensi."""
    els = dc_el.findall(f"dc:{tag}", NS)
    return [el.text.strip() for el in els if el.text and el.text.strip()]


def _get_first_dc(dc_el, tag: str) -> str:
    vals = _get_all_dc(dc_el, tag)
    return vals[0] if vals else ""


def _filter_year(pub_date: str) -> bool:
    """Return True jika pub_date masuk rentang tahun yang dikonfigurasi."""
    if not pub_date:
        return True   # kosong tetap masuk — jangan buang data valid
    try:
        return HARVEST_YEAR_FROM <= int(pub_date[:4]) <= HARVEST_YEAR_UNTIL
    except ValueError:
        return True   # format aneh (misal "2024-Spring") tetap masuk


def _extract_identifiers(identifiers: list[str]) -> tuple[str, str]:
    """
    Pisahkan URL artikel dan DOI dari list dc:identifier.
    Handle berbagai format DOI yang dipakai jurnal Indonesia:
    - https://doi.org/10.xxxxx/yyy
    - http://dx.doi.org/10.xxxxx/yyy
    - 10.xxxxx/yyy (tanpa domain)
    """
    article_url = doi = ""
    for ident in identifiers:
        if not ident:
            continue
        ident = ident.strip()
        if "doi.org/" in ident:
            doi = ident.split("doi.org/", 1)[-1].strip()
        elif ident.startswith("10.") and "/" in ident and len(ident) > 8:
            doi = ident
        elif ident.startswith("http") and not article_url:
            # Ambil URL pertama sebagai article_url
            # (beberapa jurnal taruh DOI URL sebelum article URL)
            if "doi.org" not in ident:
                article_url = ident
            elif not doi:
                doi = ident.split("doi.org/", 1)[-1].strip()
    return article_url, doi


# ── OAI error code handler ─────────────────────────────────────────────────────

# Error OAI yang berarti "tidak ada data" → selesai normal, bukan error
OAI_DONE_CODES = {"noRecordsMatch", "noSetHierarchy"}

# Error OAI yang berarti token expired → perlu restart dari awal
OAI_TOKEN_EXPIRED_CODES = {"badResumptionToken", "badArgument"}


# ── Record parser ──────────────────────────────────────────────────────────────

def parse_records(xml_bytes: bytes) -> tuple[list[dict], str | None, str | None]:
    """
    Parse XML OAI-PMH ListRecords response.

    Return: (artikel_lolos_filter, resumption_token_or_None, oai_error_code_or_None)

    oai_error_code diisi kalau ada <error> di response:
    - noRecordsMatch → selesai normal
    - badResumptionToken → token expired, perlu restart
    - lainnya → error sebenarnya
    """
    articles = []

    root = _safe_parse_xml(xml_bytes)
    if root is None:
        logger.error("XML tidak bisa di-parse sama sekali, skip halaman ini")
        return [], None, "parse_error"

    # Cek error OAI-PMH
    error_el = root.find(".//oai:error", NS)
    if error_el is not None:
        code = error_el.get("code", "unknown")
        msg  = (error_el.text or "").strip()
        if code in OAI_DONE_CODES:
            logger.info(f"OAI [{code}]: tidak ada record — selesai normal")
        elif code in OAI_TOKEN_EXPIRED_CODES:
            logger.warning(f"OAI [{code}]: {msg} — token expired, akan restart dari awal")
        else:
            logger.warning(f"OAI error [{code}]: {msg}")
        return [], None, code

    # Parse tiap record
    for record in root.findall(".//oai:record", NS):
        header = record.find("oai:header", NS)

        # Skip record deleted
        if header is not None and header.get("status") == "deleted":
            continue

        id_el  = header.find("oai:identifier", NS) if header is not None else None
        oai_id = id_el.text.strip() if id_el is not None and id_el.text else ""

        dc = record.find(".//oai_dc:dc", NS)
        if dc is None:
            continue

        pub_date = _get_first_dc(dc, "date")

        # Filter tahun publikasi
        if not _filter_year(pub_date):
            continue

        identifiers          = _get_all_dc(dc, "identifier")
        article_url, doi     = _extract_identifiers(identifiers)

        articles.append({
            "publisher_name": _get_first_dc(dc, "publisher"),
            "sinta_grade":    "",   # diisi saat harvest_journal
            "article_title":  _get_first_dc(dc, "title"),
            "authors":        "; ".join(_get_all_dc(dc, "creator")),
            "abstract":       _get_first_dc(dc, "description"),
            "keywords":       "; ".join(_get_all_dc(dc, "subject")),
            "pub_date":       pub_date,
            "article_url":    article_url,
            "doi":            doi,
            "oai_identifier": oai_id,
        })

    # resumptionToken — string kosong dianggap habis (sesuai spec OAI-PMH)
    token_el         = root.find(".//oai:resumptionToken", NS)
    resumption_token = None
    if token_el is not None and token_el.text and token_el.text.strip():
        resumption_token = token_el.text.strip()

    return articles, resumption_token, None


# ── Journal harvester ──────────────────────────────────────────────────────────

def harvest_journal(journal: dict, progress: dict, paths: dict) -> int:
    """
    Harvest semua artikel dari satu jurnal via OAI-PMH.

    Edge case yang di-handle:
    - HTTP 429/503: backoff otomatis
    - XML encoding rusak: fallback repair
    - badResumptionToken: restart dari halaman pertama
    - Thread-safe: semua akses progress via update_progress()

    Return: jumlah artikel yang di-harvest pada run ini.
    """
    oai_url       = journal["oai_url"]
    name          = journal["nama_jurnal"]
    grade         = journal["sinta_grade"]
    articles_csv  = paths["articles_csv"]
    progress_json = paths["progress_json"]

    # Baca state dengan lock
    with _progress_lock:
        state = progress.get(oai_url, {})

    if state.get("status") == "done":
        logger.info(f"[skip] {name} sudah selesai ({state.get('articles_harvested', 0)} artikel)")
        return 0

    resumption_token = state.get("resumption_token") or None
    total_harvested  = state.get("articles_harvested", 0)
    page             = state.get("page", 1)
    restart_count    = 0   # berapa kali restart karena token expired
    MAX_RESTARTS     = 2

    session = requests.Session()
    session.headers.update(HEADERS)
    batch   = []

    logger.info(f"[harvest] {name} | Sinta {grade} | {oai_url}")
    if resumption_token:
        logger.info(f"  Resume dari halaman {page}, token: {resumption_token[:40]}...")

    while True:
        url = build_list_records_url(oai_url, resumption_token)
        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

        # ── Fetch ─────────────────────────────────────────────────────────────
        try:
            resp = _get_oai_raw(session, url)
        except Exception as e:
            logger.error(f"  [{name}] Gagal fetch halaman {page}: {e}")
            if batch:
                write_articles_batch(batch, articles_csv)
                batch = []
            update_progress(progress, oai_url, {
                "status":             "error",
                "resumption_token":   resumption_token,
                "articles_harvested": total_harvested,
                "page":               page,
                "error":              str(e)[:200],
            }, progress_json)
            break

        # ── Parse XML ─────────────────────────────────────────────────────────
        articles, next_token, oai_error = parse_records(resp.content)

        # Handle token expired → restart dari awal
        if oai_error in OAI_TOKEN_EXPIRED_CODES:
            if restart_count < MAX_RESTARTS:
                restart_count   += 1
                resumption_token = None
                page             = 1
                logger.warning(
                    f"  [{name}] Token expired, restart dari awal "
                    f"(attempt {restart_count}/{MAX_RESTARTS})"
                )
                time.sleep(5)
                continue
            else:
                logger.error(f"  [{name}] Token expired {MAX_RESTARTS}x, tandai error")
                update_progress(progress, oai_url, {
                    "status":             "error",
                    "resumption_token":   None,
                    "articles_harvested": total_harvested,
                    "page":               page,
                    "error":              f"badResumptionToken after {MAX_RESTARTS} restarts",
                }, progress_json)
                break

        # Handle error lain (bukan token expired)
        if oai_error and oai_error not in OAI_DONE_CODES and oai_error != "parse_error":
            logger.error(f"  [{name}] OAI error [{oai_error}], hentikan harvest")
            if batch:
                write_articles_batch(batch, articles_csv)
                batch = []
            update_progress(progress, oai_url, {
                "status":             "error",
                "resumption_token":   None,
                "articles_harvested": total_harvested,
                "page":               page,
                "error":              oai_error,
            }, progress_json)
            break

        # ── Proses artikel ────────────────────────────────────────────────────
        for art in articles:
            art["sinta_grade"] = grade
            if not art["publisher_name"]:
                art["publisher_name"] = name

        batch.extend(articles)
        total_harvested += len(articles)

        logger.info(
            f"  [{name}] Hal {page}: {len(articles)} lolos filter "
            f"({HARVEST_YEAR_FROM}–{HARVEST_YEAR_UNTIL}) | total: {total_harvested}"
        )

        # Tulis ke CSV kalau batch penuh
        if len(batch) >= CSV_BATCH_SIZE:
            write_articles_batch(batch, articles_csv)
            batch = []

        page += 1

        # Simpan progress setiap halaman (atomic write via tmp file)
        update_progress(progress, oai_url, {
            "status":             "in_progress",
            "resumption_token":   next_token,
            "articles_harvested": total_harvested,
            "page":               page,
        }, progress_json)

        # ── Cek selesai ───────────────────────────────────────────────────────
        if not next_token:
            if batch:
                write_articles_batch(batch, articles_csv)
            update_progress(progress, oai_url, {
                "status":             "done",
                "resumption_token":   None,
                "articles_harvested": total_harvested,
                "page":               page,
            }, progress_json)
            logger.info(f"  [{name}] Selesai: {total_harvested} artikel total")
            break

        resumption_token = next_token

    return total_harvested


# ── Entry point ────────────────────────────────────────────────────────────────

async def run_phase3():
    """Entry point Fase 3."""
    paths = PATHS

    if not paths["journals_oai_active"].exists():
        logger.error(
            f"File tidak ditemukan: {paths['journals_oai_active']}. "
            "Jalankan Fase 2 terlebih dahulu."
        )
        return

    journals = []
    with open(paths["journals_oai_active"], encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("|")
            if len(parts) >= 4:
                journals.append({
                    "nama_jurnal": parts[0].strip(),
                    "url":         parts[1].strip(),
                    "sinta_grade": parts[2].strip(),
                    "oai_url":     parts[3].strip(),
                })
            else:
                logger.warning(f"Format baris tidak valid, skip: {line}")

    if not journals:
        logger.warning("Tidak ada jurnal di journals_publisher_oai_active.txt")
        return

    logger.info(f"Phase 3 mulai: {len(journals)} jurnal")
    logger.info(f"Filter pub_date: {HARVEST_YEAR_FROM}–{HARVEST_YEAR_UNTIL}")

    init_articles_csv(paths["articles_csv"])
    progress = load_progress(paths["progress_json"])

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOURNALS)
    loop      = asyncio.get_event_loop()

    async def process_one(journal):
        async with semaphore:
            return await loop.run_in_executor(None, harvest_journal, journal, progress, paths)

    results = await asyncio.gather(
        *[process_one(j) for j in journals],
        return_exceptions=True,
    )

    total   = 0
    errors  = 0
    for r in results:
        if isinstance(r, Exception):
            logger.error(f"Worker exception: {r}")
            errors += 1
        elif isinstance(r, int):
            total += r

    logger.info("=" * 50)
    logger.info(f"Phase 3 selesai: {total} artikel, {errors} error")
    logger.info(f"Output: {paths['articles_csv']}")
    logger.info("=" * 50)


if __name__ == "__main__":
    asyncio.run(run_phase3())
