import pandas as pd
import requests
from io import StringIO

from modules.config import PROXIES, CA_BUNDLE


UNICREDIT_CSV_URL = (
    "https://raw.githubusercontent.com/mikimouse-beep/FundNavRSDECL/main/unicredit.csv"
)

COLUMNS = ["Post_Date", "Sklad", "Amount", "PricePerUnit", "Num"]


def fetch_unicredit():
    try:
        print(f"Pridobivam UniCredit CSV: {UNICREDIT_CSV_URL}")

        resp = requests.get(
            UNICREDIT_CSV_URL,
            proxies=PROXIES,
            timeout=60,
            verify=CA_BUNDLE,
        )
        resp.raise_for_status()

        df = pd.read_csv(StringIO(resp.text), encoding="utf-8-sig")

        missing = set(COLUMNS) - set(df.columns)
        if missing:
            print(f"UniCredit CSV nima pričakovanih stolpcev: {missing}")
            return pd.DataFrame(columns=COLUMNS)

        df["Post_Date"] = pd.to_datetime(df["Post_Date"], errors="coerce")
        for col in ["Amount", "PricePerUnit", "Num"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = (
            df[COLUMNS]
            .dropna(subset=["Post_Date"])
            .sort_values(["Sklad", "Post_Date"])
            .reset_index(drop=True)
        )

        print(f"UniCredit uspešno prebranih vrstic: {len(df)}")
        return df

    except Exception as e:
        print(f"Napaka pri branju UniCredit CSV: {e}")
        return pd.DataFrame(columns=COLUMNS)


def fetch_unicredit_all():
    df = fetch_unicredit()

    if not df.empty:
        return [df]

    return []


if __name__ == "__main__":
    frames = fetch_unicredit_all()

    if frames:
        print(frames[0].tail())
    else:
        print("Ni podatkov.")
