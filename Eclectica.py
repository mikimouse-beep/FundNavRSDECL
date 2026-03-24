import csv
import io
import os
import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MASTER_CSV_PATH = os.path.join(BASE_DIR, "eclectica_master_history.csv")

FUNDS = [
    {
        "fund_name": "Eclectica RSD Cash UCITS Fund",
        "fund_ccy": "RSD",
        "url": "https://www.eclecticacapital.com/eclectica-rsd-cash-ucits-fund",
    },
    {
        "fund_name": "Eclectica Euro Cash UCITS Fund",
        "fund_ccy": "EUR",
        "url": "https://www.eclecticacapital.com/eclectica-euro-cash-ucits-fund",
    },
]

# Uradni NBS Excel vir za exchange rates
NBS_XLSX_URL = "https://www.nbs.rs/export/sites/NBS_site/documents/statistika/ino_ekonomski_odnosi/SBEOI09.xlsx"


def parse_number(s: str) -> float:
    s = s.strip().replace(".", "").replace(",", ".")
    return float(s)


def to_iso_date(sr_date: str) -> str:
    dd, mm, yyyy = sr_date.split(".")
    return f"{yyyy}-{mm}-{dd}"


def fetch_fund_data(fund: dict) -> dict:
    r = requests.get(fund["url"], timeout=30)
    r.raise_for_status()
    html = r.text

    soup = BeautifulSoup(html, "html.parser")
    kpis = soup.select(".fund-kpi")
    if len(kpis) < 2:
        raise ValueError(f"KPI blokov nisem našel za {fund['fund_name']}.")

    vep_block = kpis[0]
    aum_block = kpis[1]

    date_text = vep_block.select_one(".fund-kpi-sub").get_text(" ", strip=True)
    vep_text = vep_block.select_one(".fund-kpi-value").get_text(" ", strip=True)
    aum_text = aum_block.select_one(".fund-kpi-value").get_text(" ", strip=True)

    date_match = re.search(r"(\d{2}\.\d{2}\.\d{4})", date_text)
    if not date_match:
        raise ValueError(f"Datuma nisem našel za {fund['fund_name']}.")

    vep_match = re.search(r"([\d\.,]+)", vep_text)
    aum_match = re.search(r"([\d\.,]+)", aum_text)
    if not vep_match or not aum_match:
        raise ValueError(f"VEP ali AUM ni bil najden za {fund['fund_name']}.")

    sr_date = date_match.group(1)
    iso_date = to_iso_date(sr_date)

    vep = parse_number(vep_match.group(1))
    aum = parse_number(aum_match.group(1))
    units_est = aum / vep

    return {
        "date": iso_date,
        "fund_name": fund["fund_name"],
        "fund_ccy": fund["fund_ccy"],
        "vep": vep,
        "aum": aum,
        "units_est": units_est,
    }


def normalize_excel_date(value):
    if isinstance(value, datetime):
        return value.date().isoformat()

    if isinstance(value, str):
        value = value.strip()

        for fmt in ("%d.%m.%Y", "%d.%m.%Y.", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, fmt).date().isoformat()
            except ValueError:
                pass

    return None


def fetch_eur_rsd_from_nbs(target_date: str) -> float:
    """
    Vrne uradni srednji tečaj EUR/RSD za podani datum v formatu YYYY-MM-DD.
    Bere iz NBS webapp strani 'na željeni dan'.
    """
    yyyy, mm, dd = target_date.split("-")
    sr_date = f"{dd}.{mm}.{yyyy}"

    url = "https://webappcenter.nbs.rs/ExchangeRateWebApp/ExchangeRate/IndexByDate"
    params = {
        "isSearchExecuted": "true",
        "SearchDate": sr_date,
    }

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()

    html = r.text
    soup = BeautifulSoup(html, "html.parser")

    text = soup.get_text("\n", strip=True)

    # Poskusi najti vrstico za EUR in tečaj za 1 enoto
    m = re.search(r"\bEUR\b.*?\b1\b\s+(\d+,\d+)", text, flags=re.S)
    if not m:
        raise ValueError(f"NBS tečaja EUR/RSD za datum {target_date} nisem našel.")

    return float(m.group(1).replace(",", "."))

def enrich_with_fx(row: dict) -> dict:
    """
    Doda FX in RSD protivrednosti.
    Za RSD sklad pusti EUR/RSD prazen, vep_rsd in aum_rsd pa enaka originalu.
    Za EUR sklad potegne NBS EUR/RSD in preračuna.
    """
    out = dict(row)

    if row["fund_ccy"] == "RSD":
        out["eur_rsd_nbs"] = ""
        out["vep_rsd"] = round(row["vep"], 5)
        out["aum_rsd"] = round(row["aum"], 2)
    elif row["fund_ccy"] == "EUR":
        eur_rsd = fetch_eur_rsd_from_nbs(row["date"])
        out["eur_rsd_nbs"] = round(eur_rsd, 6)
        out["vep_rsd"] = round(row["vep"] * eur_rsd, 5)
        out["aum_rsd"] = round(row["aum"] * eur_rsd, 2)
    else:
        raise ValueError(f"Neznana valuta sklada: {row['fund_ccy']}")

    return out


def detect_delimiter(path: str) -> str:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        first_line = f.readline()
    if ";" in first_line:
        return ";"
    if "," in first_line:
        return ","
    return ";"


def append_if_new_master(row: dict):
    exists = os.path.exists(MASTER_CSV_PATH)
    existing_keys = set()

    fieldnames = [
        "date",
        "fund_name",
        "fund_ccy",
        "vep",
        "aum",
        "units_est",
        "eur_rsd_nbs",
        "vep_rsd",
        "aum_rsd",
    ]

    if exists:
        delimiter = detect_delimiter(MASTER_CSV_PATH)

        with open(MASTER_CSV_PATH, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f, delimiter=delimiter)

            if reader.fieldnames:
                reader.fieldnames = [name.lstrip("\ufeff").strip() for name in reader.fieldnames]

            if not reader.fieldnames or "date" not in reader.fieldnames or "fund_name" not in reader.fieldnames:
                print("NAPAKA: master CSV header ni pravilen.")
                print("Najden header:", reader.fieldnames)
                print("Pobriši stari CSV in ponovno zaženi skripto.")
                return

            for r in reader:
                key = (r.get("date", "").strip(), r.get("fund_name", "").strip())
                if key[0] and key[1]:
                    existing_keys.add(key)

    row_key = (row["date"], row["fund_name"])
    if row_key in existing_keys:
        print(f"{row['fund_name']} | {row['date']} že obstaja, nič ne dodam.")
        return

    with open(MASTER_CSV_PATH, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        if not exists:
            writer.writeheader()
        writer.writerow(row)

    print("Dodano:", row["fund_name"], row["date"])


def main():
    for fund in FUNDS:
        try:
            base_row = fetch_fund_data(fund)
            full_row = enrich_with_fx(base_row)
            append_if_new_master(full_row)
        except Exception as e:
            print(f"NAPAKA pri skladu {fund['fund_name']}: {e}")


if __name__ == "__main__":
    main()
