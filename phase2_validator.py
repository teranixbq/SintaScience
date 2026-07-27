"""
Fase 2 — Validasi OJS + OAI-PMH per jurnal
Input:  {run_dir}/journals_raw.csv
Output: 4 file klasifikasi di {run_dir}/
"""

import asyncio
import csv
import time
import random
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import (
    PATHS,
    REQUEST_TIMEOUT,
    REQUEST_DELAY_MIN,
    REQUEST_DELAY_MAX,
    MAX_RETRIES,
    MAX_CONCURRENT_JOURNALS,
    USER_AGENT,
)
from utils.logger import get_logger
from utils.csv_writer import write_publisher_line

logger = get_logger("phase2")
HEADERS = {"User-Agent": USER_AGENT}


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
    reraise=True,
)
def _get(session: requests.Session, url: str) -> requests.Response:
    return session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)


def _build_oai_candidates(base_url: str) -> list[str]:
    """
    Bangun semua kandidat OAI URL dari base URL jurnal.
    Cover variasi instalasi OJS yang umum di Indonesia.
    """
    base = base_url.rstrip("/")
    candidates = [f"{base}/oai"]

    if "/index.php/" in base:
        root = base.split("/index.php/")[0]
        if root != base:
            candidates.append(f"{root}/oai")
            candidates.append(f"{root}/index.php/oai")

    candidates.append(f"{base}/?page=oai")

    seen, unique = set(), []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            unique.append(c)
    return unique


def detect_ojs(session: requests.Session, url: str) -> tuple[bool, str]:
    """
    Deteksi OJS via:
    1. <meta name="generator" content="Open Journal Systems ...">
    2. Feed links khas OJS (WebFeedGatewayPlugin)
    3. Hint string khas OJS di HTML
    """
    try:
        resp     = _get(session, url)
        final    = str(resp.url)
        soup     = BeautifulSoup(resp.text, "lxml")

        generator = soup.find("meta", attrs={"name": "generator"})
        if generator and "open journal systems" in generator.get("content", "").lower():
            logger.debug(f"OJS via meta generator: {final}")
            return True, final

        for link in soup.find_all("link", attrs={"type": True}):
            href = link.get("href", "")
            if "xml" in link.get("type", "") and (
                "WebFeedGatewayPlugin" in href or "AnnouncementFeedGatewayPlugin" in href
            ):
                logger.debug(f"OJS via feed link: {final}")
                return True, final

        for hint in ["gateway/plugin/", "/index.php/", "pkp_context_"]:
            if hint in resp.text:
                logger.debug(f"OJS via hint '{hint}': {final}")
                return True, final

        return False, final

    except (requests.Timeout, requests.ConnectionError):
        raise
    except Exception as e:
        logger.warning(f"detect_ojs error [{url}]: {e}")
        return False, url


def detect_oai(session: requests.Session, base_url: str) -> tuple[bool, str]:
    """Coba semua kandidat OAI URL, return (aktif, oai_url)."""
    for oai_url in _build_oai_candidates(base_url):
        try:
            resp = session.get(
                f"{oai_url}?verb=Identify",
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )
            if resp.status_code == 200 and "<repositoryName>" in resp.text and "<OAI-PMH" in resp.text:
                logger.debug(f"OAI aktif: {oai_url}")
                return True, oai_url
        except Exception:
            continue
    return False, ""


def validate_journal(journal: dict, paths: dict) -> dict:
    """Validasi satu jurnal: OJS check → OAI check → kategorisasi ke file output."""
    url   = journal["url"]
    name  = journal["nama_jurnal"]
    grade = journal["sinta_grade"]

    session = _make_session()
    time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))

    try:
        is_ojs, final_url = detect_ojs(session, url)

        if not is_ojs:
            logger.info(f"[non-OJS]    {name} | {url}")
            write_publisher_line(paths["journals_non_ojs"], f"{name}|{final_url}|{grade}")
            return {**journal, "method": "non_ojs", "oai_url": ""}

        oai_active, oai_url = detect_oai(session, final_url)

        if oai_active:
            logger.info(f"[OAI aktif]  {name} | {oai_url}")
            write_publisher_line(paths["journals_oai_active"], f"{name}|{final_url}|{grade}|{oai_url}")
            return {**journal, "method": "oai", "oai_url": oai_url}
        else:
            logger.info(f"[OJS no-OAI] {name} | {final_url}")
            write_publisher_line(paths["journals_ojs_no_oai"], f"{name}|{final_url}|{grade}|")
            return {**journal, "method": "ojs_no_oai", "oai_url": ""}

    except (requests.Timeout, requests.ConnectionError) as e:
        err = "timeout" if isinstance(e, requests.Timeout) else "connection_error"
        logger.warning(f"[error]      {name} | {url} | {err}")
        write_publisher_line(paths["journals_error"], f"{name}|{url}|{grade}|{err}")
        return {**journal, "method": "error", "oai_url": "", "error": err}

    except Exception as e:
        err = str(e)[:120]
        logger.error(f"[error]      {name} | {url} | {err}")
        write_publisher_line(paths["journals_error"], f"{name}|{url}|{grade}|{err}")
        return {**journal, "method": "error", "oai_url": "", "error": err}


async def run_phase2():
    """Entry point Fase 2."""
    paths = PATHS

    raw_csv = paths["journals_raw_csv"]
    if not raw_csv.exists():
        logger.error("journals_raw.csv tidak ditemukan. Jalankan Fase 1 terlebih dahulu.")
        return

    journals = []
    with open(raw_csv, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            journals.append(dict(row))

    if not journals:
        logger.warning("journals_raw.csv kosong")
        return

    logger.info(f"Phase 2 mulai: {len(journals)} jurnal")

    # Reset file output
    for key in ["journals_oai_active", "journals_ojs_no_oai", "journals_non_ojs", "journals_error"]:
        paths[key].write_text("", encoding="utf-8")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOURNALS)
    loop      = asyncio.get_event_loop()

    async def process_one(journal):
        async with semaphore:
            return await loop.run_in_executor(None, validate_journal, journal, paths)

    results = await asyncio.gather(
        *[process_one(j) for j in journals],
        return_exceptions=True,
    )

    summary = {"oai": 0, "ojs_no_oai": 0, "non_ojs": 0, "error": 0}
    for r in results:
        if isinstance(r, Exception):
            summary["error"] += 1
        else:
            key = r.get("method", "error")
            summary[key] = summary.get(key, 0) + 1

    logger.info("=" * 50)
    logger.info("Phase 2 selesai:")
    logger.info(f"  OAI aktif    : {summary.get('oai', 0)}")
    logger.info(f"  OJS no-OAI   : {summary.get('ojs_no_oai', 0)}")
    logger.info(f"  Non-OJS      : {summary.get('non_ojs', 0)}")
    logger.info(f"  Error        : {summary.get('error', 0)}")
    logger.info("=" * 50)

    return results


if __name__ == "__main__":
    asyncio.run(run_phase2())
