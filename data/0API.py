import os
import time
import urllib.request
import ssl

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INN_FILE = os.path.join(BASE_DIR, "inn.txt")
API_FOLDER = os.path.join(BASE_DIR, "api")
os.makedirs(API_FOLDER, exist_ok=True)

ctx = ssl._create_unverified_context()

with open(INN_FILE, "r") as f:
    inns = [line.strip() for line in f if line.strip()]

total = len(inns)
print(f"Загрузка {total} компаний...")

for i, inn in enumerate(inns, 1):
    path = os.path.join(API_FOLDER, f"{inn}.json")

    if os.path.exists(path):
        continue

    print(f"[{i}/{total}] {inn}...", end="\r")

    try:
        url = f"https://egrul.itsoft.ru/{inn}.json"
        with urllib.request.urlopen(url, context=ctx, timeout=10) as resp:
            with open(path, "wb") as f:
                f.write(resp.read())
        time.sleep(1.5)
    except Exception:
        print(f"\nОшибка ИНН: {inn}")

print(f"\nГотово. Файлы в папке: {API_FOLDER}")