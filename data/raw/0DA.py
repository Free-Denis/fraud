import os
import xml.etree.ElementTree as ET
import csv
import urllib.request
import zipfile
import io
import ssl

CSV_URL = "https://fas.gov.ru/opendata/7703516539-rnp/data-2021-03-17T00-00-structure-2019-05-22T00-00.csv"
ZIP_URL = "https://file.nalog.ru/opendata/7707329152-sshr2019/data-20251225-structure-20200408.zip"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "rnp.csv")
XML_FOLDER = os.path.join(BASE_DIR, "xml")
OUT_INN_PATH = os.path.join(BASE_DIR, "inn.txt")

ctx = ssl._create_unverified_context()

if not os.path.exists(CSV_PATH):
    urllib.request.urlretrieve(CSV_URL, CSV_PATH, context=ctx)

if not os.path.exists(XML_FOLDER) or not os.listdir(XML_FOLDER):
    os.makedirs(XML_FOLDER, exist_ok=True)
    resp = urllib.request.urlopen(ZIP_URL, context=ctx)
    with zipfile.ZipFile(io.BytesIO(resp.read())) as z:
        z.extractall(XML_FOLDER)

supplier_inns = set()
with open(CSV_PATH, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        if len(row) > 18:
            inn = row[18].strip()
            if inn.isdigit() and len(inn) in (10, 12):
                supplier_inns.add(inn)

matched_inns = set()
for filename in os.listdir(XML_FOLDER):
    if filename.lower().endswith('.xml'):
        tree = ET.parse(os.path.join(XML_FOLDER, filename))
        for doc in tree.getroot().findall('.//Документ'):
            np = doc.find('СведНП')
            if np is not None:
                inn = np.get('ИННЮЛ', '').strip()
                if inn in supplier_inns:
                    matched_inns.add(inn)

with open(OUT_INN_PATH, 'w', encoding='utf-8') as f:
    for inn in sorted(matched_inns):
        f.write(f"{inn}\n")

print(f"Всего подрядчиков в РНП: {len(supplier_inns)}")
print(f"Совпало с данными ФНС: {len(matched_inns)}")