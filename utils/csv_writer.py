import csv
import threading
from pathlib import Path
from typing import List, Dict

from utils.logger import get_logger

logger = get_logger("csv_writer")

# Header kolom articles.csv
ARTICLES_HEADER = [
    "publisher_name",
    "sinta_grade",
    "article_title",
    "authors",
    "abstract",
    "keywords",
    "pub_date",
    "article_url",
    "doi",
    "oai_identifier",
]

_lock = threading.Lock()


def init_articles_csv(articles_csv: Path):
    """Buat file articles.csv dengan header jika belum ada."""
    if not articles_csv.exists():
        with open(articles_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=ARTICLES_HEADER)
            writer.writeheader()
        logger.info(f"Initialized articles.csv at {articles_csv}")


def write_articles_batch(articles: List[Dict], articles_csv: Path):
    """
    Tulis batch artikel ke articles.csv secara thread-safe.
    Field yang tidak ada diisi string kosong.
    """
    if not articles:
        return
    with _lock:
        with open(articles_csv, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=ARTICLES_HEADER, extrasaction="ignore")
            for article in articles:
                row = {field: article.get(field, "") for field in ARTICLES_HEADER}
                writer.writerow(row)
    logger.debug(f"Wrote {len(articles)} articles to {articles_csv.name}")


def write_publisher_line(filepath: Path, line: str):
    """
    Tulis satu baris ke file publisher .txt secara thread-safe.
    Format: nama|url|sinta_grade|oai_url
    """
    with _lock:
        with open(filepath, "a", encoding="utf-8") as f:
            f.write(line.strip() + "\n")
