"""
Выгрузка ai_hub_service.dialog_partition_YYYY_MM по одному дню через COPY (быстро).

Установка:  pip install psycopg2-binary
Запуск:     python load_month.py
Логин/пароль берутся из переменных PGUSER / PGPASSWORD, иначе спросит при старте.
"""
import getpass
import os
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import psycopg2
from psycopg2 import sql

# ---------------- настройки ----------------
YEAR, MONTH = 2026, 9
AGENT = "ai_consultant_new_chatbot_rag"
WORKERS = 1            # сколько дней качать одновременно; у роли лимит подключений, не завышай
RETRIES = 3            # попыток на один день
OUT_DIR = Path(f"dump_{YEAR}_{MONTH:02d}")
TZ = "Asia/Yekaterinburg"  # GMT+05:00, как в DBeaver, чтобы границы дней совпали
PROGRESS_EVERY = 10    # раз во сколько секунд печатать прогресс

DB = dict(
    host="prx.domclick.ru",
    port=20000,
    dbname="projectx",
    user=os.environ.get("PGUSER") or "aalnovikov",
    password=os.environ.get("PGPASSWORD") or getpass.getpass("password: "),
    options=f"-c timezone={TZ}",
    connect_timeout=30,
)

COLUMNS = ["pk", "created_at", "uid", "variables", "state", "correlation_id"]
TABLE = sql.Identifier("ai_hub_service", f"dialog_partition_{YEAR}_{MONTH:02d}")
QUERY = sql.SQL(
    "select {cols} from {table} "
    "where agent_name = %s and created_at >= %s and created_at < %s"
).format(cols=sql.SQL(", ").join(map(sql.Identifier, COLUMNS)), table=TABLE)
# -------------------------------------------

print_lock = threading.Lock()


def log(msg):
    with print_lock:
        print(msg, flush=True)


class Progress:
    """Обёртка над файлом: считает байты и раз в N секунд пишет, сколько скачано."""

    def __init__(self, f, label):
        self.f, self.label = f, label
        self.bytes = 0
        self.start = self.last = time.time()

    def write(self, data):
        self.f.write(data)
        self.bytes += len(data)
        now = time.time()
        if now - self.last >= PROGRESS_EVERY:
            self.last = now
            log(f"  {self.label}: {self.bytes / 2**20:.1f} МБ, {now - self.start:.0f} c")


def month_days():
    d = date(YEAR, MONTH, 1)
    while d.month == MONTH:
        yield d
        d += timedelta(days=1)


def dump_day(d: date):
    out = OUT_DIR / f"{d}.csv"
    if out.exists():                      # уже выгружен — пропускаем (можно перезапускать)
        return d, "уже был"

    tmp = out.with_suffix(".part")
    for attempt in range(1, RETRIES + 1):
        conn = None
        try:
            log(f"{d}: подключаюсь…")
            conn = psycopg2.connect(**DB)
            conn.set_client_encoding("UTF8")
            with conn.cursor() as cur:
                select_sql = cur.mogrify(QUERY, (AGENT, d, d + timedelta(days=1))).decode("utf-8")
                copy_sql = f"COPY ({select_sql}) TO STDOUT WITH (FORMAT csv, HEADER)"
                log(f"{d}: запрос отправлен, жду данные…")
                t0 = time.time()
                with open(tmp, "wb") as f:
                    f.write(b"\xef\xbb\xbf")          # BOM, чтобы Excel понял UTF-8
                    p = Progress(f, d)
                    cur.copy_expert(copy_sql, p, size=1 << 20)
                rows = cur.rowcount
            tmp.replace(out)
            return d, f"{rows} строк, {p.bytes / 2**20:.1f} МБ за {time.time() - t0:.0f} c"
        except Exception as e:
            log(f"  {d}: попытка {attempt} упала: {e}")
            time.sleep(10 * attempt)
        finally:
            if conn is not None:
                conn.close()
    return d, "ОШИБКА"


def merge(days):
    all_path = OUT_DIR / f"all_{YEAR}_{MONTH:02d}.csv"
    with open(all_path, "wb") as out:
        first = True
        for d in days:
            p = OUT_DIR / f"{d}.csv"
            if not p.exists():
                continue
            with open(p, "rb") as f:
                if not first:
                    f.readline()                    # пропускаем BOM + заголовок
                shutil.copyfileobj(f, out, 1 << 20)
            first = False
    log(f"Склеено в {all_path}")


def main():
    OUT_DIR.mkdir(exist_ok=True)
    days = list(month_days())
    t0 = time.time()
    failed = []

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(dump_day, d) for d in days]
        for fut in as_completed(futures):
            d, status = fut.result()
            log(f"{d}: {status}")
            if status == "ОШИБКА":
                failed.append(d)

    log(f"Готово за {time.time() - t0:.0f} c")
    if failed:
        log("Не выгрузились: " + ", ".join(map(str, sorted(failed)))
            + " — запусти скрипт ещё раз, готовые дни он пропустит.")
    else:
        merge(days)


if __name__ == "__main__":
    main()
