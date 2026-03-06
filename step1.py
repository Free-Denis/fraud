import os
import json
import xml.etree.ElementTree as ET
import pandas as pd
from sqlalchemy import create_engine

# --- НАСТРОЙКИ ПОДКЛЮЧЕНИЯ ---
# Замени 'your_password' на пароль от своего Postgres
DB_URL = 'postgresql://postgres:1@localhost:5432/fraud_db'


def process_and_upload():
    print("Начинаю обработку файлов...")

    # Определяем путь к папке, где лежит скрипт (Desktop\sber)
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # Прописываем пути в соответствии с твоей структурой
    RNP_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'rnp.csv')
    XML_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'xml')
    API_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'api')

    # 1. Читаем РНП
    rnp_inns = set()
    if not os.path.exists(RNP_PATH):
        print(f"Критическая ошибка: Файл не найден по пути {RNP_PATH}")
        return

    with open(RNP_PATH, 'r', encoding='utf-8') as f:
        import csv
        # Читаем CSV с учетом кавычек и запятых
        reader = csv.reader(f, delimiter=',', quotechar='"')
        next(reader)  # Пропуск заголовка
        for row in reader:
            if len(row) > 18:
                inn = row[18].strip()
                if inn: rnp_inns.add(inn)

    # 2. Собираем штат из XML
    staff_map = {}
    if os.path.exists(XML_PATH):
        for fn in os.listdir(XML_PATH):
            if fn.endswith('.xml'):
                try:
                    root = ET.parse(os.path.join(XML_PATH, fn)).getroot()
                    for doc in root.findall('.//Документ'):
                        np_el = doc.find('СведНП')
                        if np_el is not None:
                            inn = np_el.get('ИННЮЛ')
                            ssch_el = doc.find('СведССЧР')
                            count = ssch_el.get('КолРаб', '0') if ssch_el is not None else '0'
                            staff_map[inn] = int(count)
                except Exception as e:
                    print(f"Ошибка парсинга XML {fn}: {e}")

    # 3. Собираем профили из JSON (API)
    final_rows = []
    if os.path.exists(API_PATH):
        for fn in os.listdir(API_PATH):
            if fn.endswith('.json'):
                with open(os.path.join(API_PATH, fn), 'r', encoding='utf-8') as f:
                    try:
                        data = json.load(f)
                        d = data.get('СвЮЛ', {})
                        attr = d.get('@attributes', {})
                        inn = attr.get('ИНН')

                        # Даты
                        reg_date = pd.to_datetime(attr.get('ДатаОГРН'))
                        liq_date_str = d.get('СвПрекрЮЛ', {}).get('@attributes', {}).get('ДатаПрекр')
                        liq_date = pd.to_datetime(liq_date_str) if liq_date_str else None

                        end_date = liq_date if liq_date else pd.Timestamp.now()

                        final_rows.append({
                            'inn': inn,
                            'name': d.get('СвНаимЮЛ', {}).get('@attributes', {}).get('НаимЮЛПолн'),
                            'capital': float(d.get('СвУстКап', {}).get('@attributes', {}).get('СумКап', 0)),
                            'staff_count': staff_map.get(inn, 0),
                            'reg_date': reg_date,
                            'liq_date': liq_date,
                            'is_liquidated': 1 if liq_date else 0,
                            'age_years': round((end_date - reg_date).days / 365.25, 2),
                            'is_rnp': 1 if inn in rnp_inns else 0
                        })
                    except Exception as e:
                        print(f"Ошибка в файле {fn}: {e}")

    # Создаем DataFrame
    df = pd.DataFrame(final_rows)
    print(f"Обработка завершена. Собрано компаний: {len(df)}")

    # ЗАЛИВКА В POSTGRES
    if not df.empty:
        print("Заливаю данные в PostgreSQL...")
        try:
            engine = create_engine(DB_URL)
            df.to_sql('fraud_mart', engine, if_exists='replace', index=False)
            print("Успешно! Таблица 'fraud_mart' создана в базе.")
        except Exception as e:
            print(f"Ошибка подключения к Postgres: {e}")
            print("Проверь запущен ли сервер Postgres и правильно ли указан пароль в DB_URL.")
    else:
        print("Данных для заливки не найдено.")


if __name__ == "__main__":
    process_and_upload()