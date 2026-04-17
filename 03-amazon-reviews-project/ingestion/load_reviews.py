import gzip
import json
import os
import logging
import tempfile
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector
from tqdm import tqdm

# ── Config ────────────────────────────────────────────────────────────────────
load_dotenv(Path(__file__).parent.parent / ".env")

REVIEWS_FILE  = Path(__file__).parent.parent / "data/raw/Electronics_reviews.jsonl.gz"
METADATA_FILE = Path(__file__).parent.parent / "data/raw/meta_Electronics.jsonl.gz"
BATCH_SIZE    = 50_000  # rows per temp file chunk

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Snowflake connection ──────────────────────────────────────────────────────
def get_connection():
    return snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        role      = os.getenv("SNOWFLAKE_ROLE"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
        database  = os.getenv("SNOWFLAKE_DATABASE"),
        schema    = os.getenv("SNOWFLAKE_SCHEMA"),
    )

# ── Table setup ───────────────────────────────────────────────────────────────
def create_tables(cur):
    log.info("Creating RAW tables if not exists...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS RAW_ELECTRONICS_REVIEWS (
            raw_data    VARIANT,
            loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS RAW_ELECTRONICS_METADATA (
            raw_data    VARIANT,
            loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    log.info("Tables ready.")

# ── Stage setup ───────────────────────────────────────────────────────────────
def create_stage(cur):
    cur.execute("""
        CREATE STAGE IF NOT EXISTS RAW_LOAD_STAGE
        FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
    """)
    log.info("Stage ready.")

# ── Upload chunk via PUT + COPY INTO ─────────────────────────────────────────
def upload_chunk(cur, records: list, table: str, chunk_num: int):
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".jsonl",
        delete=False,
        encoding="utf-8"
    ) as tmp:
        for record in records:
            tmp.write(json.dumps(record) + "\n")
        tmp_path = tmp.name

    try:
        # PUT local file to Snowflake internal stage
        put_cmd = f"PUT 'file://{tmp_path.replace(chr(92), '/')}' @RAW_LOAD_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        cur.execute(put_cmd)

        # COPY INTO table from stage
        cur.execute(f"""
            COPY INTO {table} (raw_data)
            FROM (SELECT $1 FROM @RAW_LOAD_STAGE)
            FILE_FORMAT = (TYPE = 'JSON')
            PURGE = TRUE
        """)
        log.info(f"  ✓ Chunk {chunk_num}: {len(records):,} records → {table}")

    finally:
        os.unlink(tmp_path)

# ── Generic JSONL loader ──────────────────────────────────────────────────────
def load_jsonl_gz(cur, filepath: Path, table: str, limit: int = 500_000):
    log.info(f"Loading {filepath.name} → {table}")

    batch     = []
    total     = 0
    chunk_num = 0

    with gzip.open(filepath, "rt", encoding="utf-8") as f:
        for line in tqdm(f, desc=f"Reading {filepath.name}"):
            if total >= limit:
                break
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                batch.append(record)
            except json.JSONDecodeError:
                continue

            if len(batch) >= BATCH_SIZE:
                chunk_num += 1
                upload_chunk(cur, batch, table, chunk_num)
                total += len(batch)
                batch  = []

    # flush remaining
    if batch:
        chunk_num += 1
        upload_chunk(cur, batch, table, chunk_num)
        total += len(batch)

    log.info(f"✅ {table}: {total:,} total records loaded in {chunk_num} chunks")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("Connecting to Snowflake...")
    conn = get_connection()
    cur  = conn.cursor()

    try:
        create_tables(cur)
        create_stage(cur)

        load_jsonl_gz(cur, REVIEWS_FILE,  "RAW_ELECTRONICS_REVIEWS",  limit=500_000)
        load_jsonl_gz(cur, METADATA_FILE, "RAW_ELECTRONICS_METADATA", limit=100_000)

        conn.commit()
        log.info("🎉 Ingestion complete.")

    except Exception as e:
        conn.rollback()
        log.error(f"❌ Ingestion failed: {e}")
        raise

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()