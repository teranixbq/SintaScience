from pathlib import Path
from datetime import datetime

# ── Direktori ─────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
LOG_DIR  = BASE_DIR / "logs"

DATA_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# ── Semua path output (flat, tidak per-run) ───────────────────────────────────
PATHS: dict[str, Path] = {
    "journals_raw_csv":    DATA_DIR / "journals_raw.csv",
    "journals_oai_active": DATA_DIR / "journals_publisher_oai_active.txt",
    "journals_ojs_no_oai": DATA_DIR / "journals_publisher_ojs_no_oai.txt",
    "journals_non_ojs":    DATA_DIR / "journals_publisher_non_ojs.txt",
    "journals_error":      DATA_DIR / "journals_publisher_error.txt",
    "articles_csv":        DATA_DIR / "articles.csv",
    "progress_json":       DATA_DIR / "progress.json",
    "run_summary":         DATA_DIR / "run_summary.txt",
    "state_json":          DATA_DIR / "state.json",
}

# Alias untuk backward-compat (phase1 masih pakai JOURNALS_RAW_CSV langsung)
JOURNALS_RAW_CSV = PATHS["journals_raw_csv"]

# ── Log file (satu file log global) ──────────────────────────────────────────
LOG_FILE = LOG_DIR / "run.log"

# ── Target Sinta grade ────────────────────────────────────────────────────────
SINTA_GRADES = [3, 4]

# ── Default rentang tahun (5 tahun ke belakang s/d tahun ini) ────────────────
_THIS_YEAR = datetime.now().year
DEFAULT_YEAR_FROM  = _THIS_YEAR - 4   # 5 tahun: from 4 tahun lalu s/d sekarang
DEFAULT_YEAR_UNTIL = _THIS_YEAR

# Nilai aktif — di-override oleh main.py via set_harvest_years()
HARVEST_YEAR_FROM  = DEFAULT_YEAR_FROM
HARVEST_YEAR_UNTIL = DEFAULT_YEAR_UNTIL


def set_harvest_years(from_year: int, until_year: int):
    """Override rentang tahun harvest dari CLI argument."""
    global HARVEST_YEAR_FROM, HARVEST_YEAR_UNTIL
    HARVEST_YEAR_FROM  = from_year
    HARVEST_YEAR_UNTIL = until_year


# ── HTTP / scraping ───────────────────────────────────────────────────────────
REQUEST_TIMEOUT   = 30        # detik
REQUEST_DELAY_MIN = 1.0       # jeda minimum antar request (detik)
REQUEST_DELAY_MAX = 3.0       # jeda maksimum antar request (detik)
MAX_RETRIES       = 3         # retry otomatis saat timeout/error

USER_AGENT = (
    "Mozilla/5.0 (compatible; SintaHarvester/1.0; "
    "+https://github.com/sintaharv) "
    "OAI-PMH Harvester"
)

# ── Concurrency ───────────────────────────────────────────────────────────────
MAX_CONCURRENT_JOURNALS = 5   # jurnal yang diproses paralel (Fase 2 & 3)

# ── Batch tulis CSV ───────────────────────────────────────────────────────────
CSV_BATCH_SIZE = 50           # tulis ke articles.csv setiap N artikel

# ── Fase 1 ────────────────────────────────────────────────────────────────────
SINTA_BASE_URL = "https://sinta.kemdiktisaintek.go.id/journals"

# ── Subject filter (Fase 1) ───────────────────────────────────────────────────
# Mapping nama subject (CLI) → label resmi di portal Sinta
SINTA_SUBJECT_MAP: dict[str, str] = {
    "engineering": "Engineering",
    "science":     "Science",
    "education":   "Education",
    "health":      "Health",
    "agriculture": "Agriculture",
    "economy":     "Economy",
    "social":      "Social",
    "humanities":  "Humanities",
    "religion":    "Religion",
    "art":         "Art",
}

# Default: Science saja (informatika, komputer, matematika, fisika terapan)
DEFAULT_SUBJECTS: list[str] = ["science"]

# Nilai aktif — di-override oleh main.py via set_subjects()
HARVEST_SUBJECTS: list[str] = list(DEFAULT_SUBJECTS)


def set_subjects(subjects: list[str]):
    """Override subject filter dari argparse --subject."""
    global HARVEST_SUBJECTS
    cleaned = [s.lower().strip() for s in subjects]
    # Validasi
    invalid = [s for s in cleaned if s not in SINTA_SUBJECT_MAP]
    if invalid:
        valid_keys = ", ".join(SINTA_SUBJECT_MAP.keys())
        raise ValueError(f"Subject tidak dikenal: {invalid}. Pilihan valid: {valid_keys}")
    HARVEST_SUBJECTS = cleaned


def get_sinta_subject_labels() -> list[str]:
    """Return label resmi Sinta untuk subject yang aktif (dipakai di URL filter)."""
    return [SINTA_SUBJECT_MAP[s] for s in HARVEST_SUBJECTS if s in SINTA_SUBJECT_MAP]


# ── OAI-PMH ───────────────────────────────────────────────────────────────────
OAI_METADATA_PREFIX = "oai_dc"
