import csv
import os
import re
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MASTER_CSV_PATH = os.path.join(BASE_DIR, "FundNavRSDECL_master_history.csv")

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

NBS_INDEX_BY_DATE_URL = "https://webappcenter.nbs.rs/ExchangeRateWebApp/ExchangeRate/IndexByDate"

session = requests.Session()
_fx_cache = {}

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


def parse_number(s: str) -> float:
    s = s.strip().replace(".", "").replace(",", ".")
    return float(s)


def to_iso_date(sr_date: str) -> str:
    dd, mm, yyyy = sr_date.split(".")
    return f"{yyyy}-{mm}-{dd}"


def fetch_fund_data(fund: dict) -> dict:
    r = session.get(fund["url"], headers=HEADERS, timeout=30)
    r.raise_for_status()
    html = r.text

    soup = BeautifulSoup(html, "html.parser")
    kpis = soup.select(".fund-kpi")
    if len(kpis) < 2:
        raise ValueError(f"KPI blokov nisem našel za {fund['fund_name']}.")

    vep_block = kpis[0]
    aum_block = kpis[1]

    date_node = vep_block.select_one(".fund-kpi-sub")
    vep_node = vep_block.select_one(".fund-kpi-value")
    aum_node = aum_block.select_one(".fund-kpi-value")

    if not date_node or not vep_node or not aum_node:
        raise ValueError(f"Manjkajo KPI elementi za {fund['fund_name']}.")

    date_text = date_node.get_text(" ", strip=True)
    vep_text = vep_node.get_text(" ", strip=True)
    aum_text = aum_node.get_text(" ", strip=True)

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
    units_est = aum / vep if vep else None

    return {
        "date": iso_date,
        "fund_name": fund["fund_name"],
        "fund_ccy": fund["fund_ccy"],
        "vep": vep,
        "aum": aum,
        "units_est": units_est,
    }


def _extract_rate_and_formed_date(html: str):
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    # sprejmi latinico ALI cirilico
    formed_match = re.search(
        r"(?:FORMIRANA NA DAN|ФОРМИРАНА НА ДАН)\s+(\d{1,2}\.\d{1,2}\.\d{4})\.",
        text,
        flags=re.I
    )
    if not formed_match:
        return None, None

    formed_date = datetime.strptime(formed_match.group(1), "%d.%m.%Y").date()

    # EUR vrstica na strani izgleda npr.:
    # EUR 978 ЕМУ 1 117,0840 117,7886
    # Zato ne iščemo "EMU", ampak samo EUR + 978 + 1 + prvi tečaj
    rate_match = re.search(
        r"\bEUR\b\s+978\s+.*?\s+1\s+(\d+,\d+)",
        text,
        flags=re.S
    )
    if not rate_match:
        return None, None

    rate = float(rate_match.group(1).replace(",", "."))
    return rate, formed_date


def _fetch_rate_for_date(query_date):
    sr_date = query_date.strftime("%d.%m.%Y.")

    params = {
        "isSearchExecuted": "true",
        "Date": sr_date,
        "ExchangeRateListTypeID": "1",
    }

    r = session.get(NBS_INDEX_BY_DATE_URL, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()

    # debug za lažje odkrivanje težav
    print("NBS request URL:", r.url)

    rate, formed_date = _extract_rate_and_formed_date(r.text)
    print("NBS parsed:", query_date.isoformat(), "->", rate, formed_date)

    return rate, formed_date


def fetch_eur_rsd_from_nbs(target_date: str, max_lookback_days: int = 10) -> float:
    if target_date in _fx_cache:
        return _fx_cache[target_date]

    base_date = datetime.strptime(target_date, "%Y-%m-%d").date()

    for offset in range(max_lookback_days + 1):
        d = base_date - timedelta(days=offset)
        iso_date = d.isoformat()

        if iso_date in _fx_cache:
            rate = _fx_cache[iso_date]
            _fx_cache[target_date] = rate
            return rate

        rate, formed_date = _fetch_rate_for_date(d)

        if rate is None or formed_date is None:
            continue

        if formed_date <= d:
            _fx_cache[formed_date.isoformat()] = rate
            _fx_cache[target_date] = rate

            if formed_date.isoformat() != target_date:
                print(
                    f"NBS FX fallback za {target_date} -> uporabljen "
                    f"{formed_date.isoformat()}: {rate}"
                )

            return rate

    raise ValueError(
        f"NBS tečaja EUR/RSD za datum {target_date} nisem našel "
        f"(lookback {max_lookback_days} dni)."
    )


def enrich_with_fx(row: dict) -> dict:
    out = dict(row)

    if row["fund_ccy"] == "RSD":
        out["eur_rsd_nbs"] = ""
        out["vep_rsd"] = row["vep"]
        out["aum_rsd"] = row["aum"]

    elif row["fund_ccy"] == "EUR":
        eur_rsd = fetch_eur_rsd_from_nbs(row["date"])
        out["eur_rsd_nbs"] = eur_rsd
        out["vep_rsd"] = row["vep"] * eur_rsd
        out["aum_rsd"] = row["aum"] * eur_rsd

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
            print(f"\n--- START {fund['fund_name']} ---")
            base_row = fetch_fund_data(fund)
            print("BASE ROW:", base_row)

            full_row = enrich_with_fx(base_row)
            print("FULL ROW:", full_row)

            append_if_new_master(full_row)
            print(f"--- DONE {fund['fund_name']} ---")

        except Exception as e:
            print(f"NAPAKA pri skladu {fund['fund_name']}: {e}")


if __name__ == "__main__":
    main()
