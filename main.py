"""
Main orchestrator — Sinta 3-4 OAI Harvester

Usage:
  python main.py --phase 1                        # Crawl daftar jurnal dari Sinta
  python main.py --phase 2                        # Validasi OJS + OAI per jurnal
  python main.py --phase 3                        # Harvest artikel via OAI-PMH
  python main.py --phase 1 2 3                    # Jalankan semua fase berurutan
  python main.py --all                            # Sama dengan --phase 1 2 3

  python main.py --phase 2 3 --from-year 2020 --to-year 2024
  python main.py --all --from-year 2019           # from 2019 sampai tahun ini
  python main.py --phase 1 --subject engineering science
  python main.py --all --subject engineering      # hanya jurnal Engineering
"""

import asyncio
import argparse
import sys
from datetime import datetime

import config
from config import (
    PATHS,
    set_harvest_years,
    set_subjects,
    DEFAULT_YEAR_FROM,
    DEFAULT_YEAR_UNTIL,
    DEFAULT_SUBJECTS,
    SINTA_SUBJECT_MAP,
    LOG_FILE,
    DATA_DIR,
)
from utils.logger import get_logger

logger = get_logger("main")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Sinta 3-4 OAI Harvester",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # ── Fase ──────────────────────────────────────────────────────────────────
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--phase",
        nargs="+",
        type=int,
        choices=[1, 2, 3],
        metavar="N",
        help="Fase yang dijalankan (1, 2, 3 atau kombinasi)",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Jalankan semua fase (1 → 2 → 3)",
    )

    # ── Tahun dinamis ─────────────────────────────────────────────────────────
    parser.add_argument(
        "--from-year",
        type=int,
        default=DEFAULT_YEAR_FROM,
        metavar="YYYY",
        help=f"Tahun awal artikel yang diambil (default: {DEFAULT_YEAR_FROM})",
    )
    parser.add_argument(
        "--to-year",
        type=int,
        default=DEFAULT_YEAR_UNTIL,
        metavar="YYYY",
        help=f"Tahun akhir artikel yang diambil (default: {DEFAULT_YEAR_UNTIL})",
    )

    # ── Subject filter (Fase 1) ───────────────────────────────────────────────
    valid_subjects = ", ".join(SINTA_SUBJECT_MAP.keys())
    parser.add_argument(
        "--subject",
        nargs="+",
        default=list(DEFAULT_SUBJECTS),
        metavar="SUBJ",
        help=(
            f"Bidang ilmu jurnal untuk Fase 1 (default: {' '.join(DEFAULT_SUBJECTS)}). "
            f"Pilihan: {valid_subjects}"
        ),
    )

    # ── Sandbox mode (dipakai GitHub Actions manual.yml) ─────────────────────
    parser.add_argument(
        "--sandbox",
        type=str,
        default=None,
        metavar="DIR",
        help=(
            "Jalankan dalam mode sandbox: baca jurnal dari DIR/journals_sample.txt, "
            "tulis output ke DIR/. Tidak update state.json."
        ),
    )

    return parser.parse_args()


def write_run_summary(phases: list, from_year: int, to_year: int,
                      started_at: datetime, finished_at: datetime,
                      subjects: list[str] | None = None):
    """Tulis ringkasan run lengkap ke data/run_summary.txt."""
    import csv as _csv
    import json as _json
    from collections import Counter

    paths    = PATHS
    duration = finished_at - started_at
    minutes, seconds = divmod(int(duration.total_seconds()), 60)

    def count_lines(fp):
        try:
            return sum(1 for l in open(fp, encoding="utf-8") if l.strip())
        except Exception:
            return 0

    # Statistik artikel
    rows = []
    if paths["articles_csv"].exists():
        try:
            with open(paths["articles_csv"], newline="", encoding="utf-8") as f:
                rows = list(_csv.DictReader(f))
        except Exception:
            pass

    years      = Counter(r["pub_date"][:4] for r in rows if r.get("pub_date"))
    grades     = Counter(r["sinta_grade"] for r in rows if r.get("sinta_grade"))
    publishers = Counter(r["publisher_name"] for r in rows if r.get("publisher_name"))

    # Progress per jurnal
    progress = {}
    if paths["progress_json"].exists():
        try:
            with open(paths["progress_json"], encoding="utf-8") as f:
                progress = _json.load(f)
        except Exception:
            pass

    lines = [
        "=" * 60,
        "SINTA 3-4 OAI HARVESTER — RUN SUMMARY",
        "=" * 60,
        f"Started     : {started_at.strftime('%Y-%m-%d %H:%M:%S')}",
        f"Finished    : {finished_at.strftime('%Y-%m-%d %H:%M:%S')}",
        f"Duration    : {minutes}m {seconds}s",
        f"Phases      : {phases}",
        f"Year range  : {from_year} – {to_year}",
        f"Subject     : {', '.join(subjects) if subjects else 'science'}",
        f"Output dir  : {DATA_DIR}",
        "-" * 60,
        f"Total artikel: {len(rows)}",
        "",
        "Per tahun:",
    ]
    for y in sorted(years):
        lines.append(f"  {y}: {years[y]} artikel")

    lines += ["", "Per Sinta grade:"]
    for g in sorted(grades):
        lines.append(f"  Sinta {g}: {grades[g]} artikel")

    lines += ["", "Per publisher (top 10):"]
    for pub, cnt in publishers.most_common(10):
        lines.append(f"  [{cnt:4d}] {pub}")

    lines += ["", "Klasifikasi jurnal:"]
    for label, key in [
        ("OAI aktif   ", "journals_oai_active"),
        ("OJS no-OAI  ", "journals_ojs_no_oai"),
        ("Non-OJS     ", "journals_non_ojs"),
        ("Error       ", "journals_error"),
    ]:
        lines.append(f"  {label}: {count_lines(paths[key])} jurnal")

    if progress:
        lines += ["", "Progress per jurnal:"]
        for url, state in progress.items():
            parts = url.rstrip("/").split("/")
            jname = parts[-2] if parts[-1] == "oai" else parts[-1]
            lines.append(
                f"  {state.get('status','?'):12} | "
                f"{state.get('articles_harvested', 0):4} artikel | {jname}"
            )

    lines.append("=" * 60)

    summary_text = "\n".join(lines)
    paths["run_summary"].write_text(summary_text + "\n", encoding="utf-8")
    return summary_text


async def main():
    args    = parse_args()
    phases  = sorted(set(args.phase)) if args.phase else [1, 2, 3]

    # ── Validasi tahun ────────────────────────────────────────────────────────
    from_year = args.from_year
    to_year   = args.to_year
    if from_year > to_year:
        print(f"Error: --from-year ({from_year}) tidak boleh lebih besar dari --to-year ({to_year})")
        sys.exit(1)

    # ── Set rentang tahun di config (global) ──────────────────────────────────
    set_harvest_years(from_year, to_year)

    # ── Set subject filter di config (global) ─────────────────────────────────
    subjects = [s.lower().strip() for s in args.subject]
    try:
        set_subjects(subjects)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # ── Sandbox mode ──────────────────────────────────────────────────────────
    sandbox_dir = None
    if args.sandbox:
        from pathlib import Path
        sandbox_dir = Path(args.sandbox)
        sandbox_dir.mkdir(parents=True, exist_ok=True)

        # Override PATHS agar semua output masuk ke sandbox_dir
        # journals_oai_active di-override ke journals_sample.txt (10 jurnal)
        sample_file = sandbox_dir / "journals_sample.txt"
        config.PATHS.update({
            "journals_oai_active": sample_file,
            "journals_ojs_no_oai": sandbox_dir / "journals_publisher_ojs_no_oai.txt",
            "journals_non_ojs":    sandbox_dir / "journals_publisher_non_ojs.txt",
            "journals_error":      sandbox_dir / "journals_publisher_error.txt",
            "articles_csv":        sandbox_dir / "articles.csv",
            "progress_json":       sandbox_dir / "progress.json",
            "run_summary":         sandbox_dir / "run_summary.txt",
        })
        # Jangan update state_json — sandbox tidak update state global
        logger.info(f"SANDBOX MODE aktif: output → {sandbox_dir}")
        logger.info(f"Jurnal input: {sample_file}")

    started_at = datetime.now()

    # ── Logging ───────────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Sinta 3-4 OAI Harvester")
    logger.info(f"Output dir : {DATA_DIR}")
    logger.info(f"Fase       : {phases}")
    logger.info(f"Tahun      : {from_year} – {to_year}")
    logger.info(f"Subject    : {subjects}")
    logger.info(f"Started    : {started_at.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    # ── Jalankan fase ─────────────────────────────────────────────────────────
    if 1 in phases:
        logger.info(">>> FASE 1: Crawl daftar jurnal dari portal Sinta")
        from phase1_sinta_crawler import run_phase1
        await run_phase1(subjects=subjects)

    if 2 in phases:
        logger.info(">>> FASE 2: Validasi OJS + OAI-PMH per jurnal")
        from phase2_validator import run_phase2
        await run_phase2()

    if 3 in phases:
        logger.info(">>> FASE 3: Harvest artikel via OAI-PMH")
        from phase3_harvester import run_phase3
        await run_phase3()

        # Update state.json setelah harvest selesai (skip di sandbox mode)
        if not sandbox_dir:
            import json as _json
            state = {
                "last_run_date": datetime.now().strftime("%Y-%m-%d"),
                "last_run_phases": phases,
                "year_from": from_year,
                "year_until": to_year,
            }
            # Hitung total artikel jika ada
            try:
                import csv as _csv
                with open(PATHS["articles_csv"], newline="", encoding="utf-8") as f:
                    state["total_articles"] = sum(1 for _ in _csv.DictReader(f))
            except Exception:
                pass
            PATHS["state_json"].write_text(
                _json.dumps(state, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8"
            )
            logger.info(f"state.json diupdate: {state}")

    # ── Ringkasan ─────────────────────────────────────────────────────────────
    finished_at  = datetime.now()
    summary_text = write_run_summary(phases, from_year, to_year,
                                     started_at, finished_at, subjects=subjects)

    logger.info("=" * 60)
    logger.info("Semua fase selesai.")
    logger.info(f"Output: {DATA_DIR}")
    logger.info("=" * 60)

    print("\n" + summary_text)


if __name__ == "__main__":
    asyncio.run(main())
