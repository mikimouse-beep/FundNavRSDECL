import csv
import os
import re
import requests
from bs4 import BeautifulSoup

URL = "https://www.eclecticacapital.com/eclectica-euro-cash-ucits-fund"


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "ecl_euro_cash_history.csv")

def parse_number(s: str) -> float:
    s = s.strip().replace(".", "").replace(",", ".")
    return float(s)

def fetch_data():
    r = requests.get(URL, timeout=30)
    r.raise_for_status()
    html = r.text

    soup = BeautifulSoup(html, "html.parser")

    kpis = soup.select(".fund-kpi")
    if len(kpis) < 2:
        raise ValueError("KPI blokov nisem našel.")

    vep_block = kpis[0]
    aum_block = kpis[1]

    date_text = vep_block.select_one(".fund-kpi-sub").get_text(" ", strip=True)
    vep_text = vep_block.select_one(".fund-kpi-value").get_text(" ", strip=True)
    aum_text = aum_block.select_one(".fund-kpi-value").get_text(" ", strip=True)

    date_match = re.search(r"(\d{2}\.\d{2}\.\d{4})", date_text)
    if not date_match:
        raise ValueError("Datuma nisem našel.")
    date_sr = date_match.group(1)
    dd, mm, yyyy = date_sr.split(".")
    iso_date = f"{yyyy}-{mm}-{dd}"

    vep_match = re.search(r"([\d\.,]+)", vep_text)
    aum_match = re.search(r"([\d\.,]+)", aum_text)
    if not vep_match or not aum_match:
        raise ValueError("VEP ali AUM ni bil najden.")

    vep = parse_number(vep_match.group(1))
    aum = parse_number(aum_match.group(1))
    units_est = aum / vep

    return {
        "date": iso_date,
        "vep_eur": vep,
        "aum_eur": aum,
        "units_est": units_est,
    }

def append_if_new(row):
    exists = os.path.exists(CSV_PATH)
    existing_dates = set()

    if exists:
        with open(CSV_PATH, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=";")
            for r in reader:
                existing_dates.add(r["date"])

    if row["date"] in existing_dates:
        print(f"Datum {row['date']} že obstaja, nič ne dodam.")
        return

    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        fieldnames = ["date", "vep_eur", "aum_eur", "units_est"]
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        if not exists:
            writer.writeheader()
        writer.writerow(row)

    print("Dodano:", row)


if __name__ == "__main__":
    row = fetch_data()
    append_if_new(row)
