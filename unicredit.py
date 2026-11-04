import json
import os
import sys
import time
from urllib.parse import urlencode

import pandas as pd
from playwright.sync_api import sync_playwright


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "unicredit.csv")

UNICREDIT_HOME = "https://www.unicreditinvest.rs/"
UNICREDIT_API = "https://www.unicreditinvest.rs/api/fund"
PAGE_SIZE = 25

COLUMNS = ["Post_Date", "Sklad", "Amount", "PricePerUnit", "Num"]

# Možna besedila gumba za piškotke / "Nadaljuj"
CONSENT_TEXTS = [
    "Nadaljuj",
    "Nastavi",
    "Nastavite",
    "Prihvati",
    "Prihvatam",
    "Slažem se",
    "Accept",
    "Continue",
]


def build_api_url(page_num):
    params = {
        "pagination[page]": page_num,
        "pagination[pageSize]": PAGE_SIZE,
        "populate[heroImage]": "true",
        "populate[documents][populate][document]": "true",
        "populate[dailyValues]": "true",
    }
    return f"{UNICREDIT_API}?{urlencode(params)}"


def click_consent(page):
    for text in CONSENT_TEXTS:
        try:
            page.get_by_text(text, exact=False).first.click(timeout=2000)
            print(f"Kliknjen gumb: {text}")
            page.wait_for_timeout(1000)
            return
        except Exception:
            continue
    print("Gumba za nadaljevanje nisem našel ali ni bil potreben.")


def fetch_json(page, url, retries=3):
    """API kličemo znotraj brskalnika, da gredo zraven piškotki in pravi headerji."""
    js = """
    async (url) => {
        const r = await fetch(url, {
            headers: { "Accept": "application/json, text/plain, */*" },
            credentials: "include",
        });
        return { status: r.status, text: await r.text() };
    }
    """

    for attempt in range(1, retries + 1):
        try:
            result = page.evaluate(js, url)
            print(f"API status: {result['status']} (poskus {attempt})")

            if result["status"] == 200:
                return json.loads(result["text"])

            print(result["text"][:300])

            # Rezerva: odpremo API URL neposredno v zavihku
            page.goto(url, wait_until="networkidle", timeout=60000)
            body = page.locator("body").inner_text()
            return json.loads(body)

        except Exception as e:
            print(f"Napaka pri klicu API: {e}")
            time.sleep(5)

    return None


def unwrap(obj):
    """Podpora za Strapi v4 (data/attributes) in v5 (ploščat format)."""
    if isinstance(obj, dict) and isinstance(obj.get("attributes"), dict):
        out = dict(obj["attributes"])
        out.setdefault("id", obj.get("id"))
        return out
    return obj


def get_daily_values(item):
    dv = item.get("dailyValues") or []
    if isinstance(dv, dict):
        dv = dv.get("data") or []
    return [unwrap(x) for x in dv]


def item_to_df(item):
    item = unwrap(item)
    sklad = item.get("name", "UNKNOWN")
    daily_values = get_daily_values(item)

    if not daily_values:
        print(f"Ni dailyValues za {sklad}")
        return pd.DataFrame(columns=COLUMNS)

    df = pd.DataFrame(daily_values)

    for col in ["date", "netAssets", "priceRSD"]:
        if col not in df.columns:
            df[col] = None

    df["Sklad"] = sklad
    # Vzamemo samo YYYY-MM-DD, da časovni pas ne premakne datuma
    df["Post_Date"] = pd.to_datetime(
        df["date"].astype(str).str[:10],
        errors="coerce",
    )
    df["Amount"] = pd.to_numeric(df["netAssets"], errors="coerce")
    df["PricePerUnit"] = pd.to_numeric(df["priceRSD"], errors="coerce")
    df["Num"] = df["Amount"] / df["PricePerUnit"].replace(0, pd.NA)

    df = df[COLUMNS].dropna(subset=["Post_Date"]).sort_values("Post_Date")

    print(f"{sklad}: prebranih vrstic {len(df)}")
    return df


def fetch_unicredit_all():
    all_frames = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="sr-RS",
        )
        page = context.new_page()

        print(f"Odpiram: {UNICREDIT_HOME}")
        page.goto(UNICREDIT_HOME, wait_until="networkidle", timeout=60000)
        click_consent(page)

        page_num = 1
        page_count = 1

        while page_num <= page_count:
            url = build_api_url(page_num)
            print(f"\n--- API stran {page_num} ---")

            payload = fetch_json(page, url)
            if not payload:
                print(f"NAPAKA: ni podatkov za stran {page_num}")
                break

            for item in payload.get("data", []):
                df = item_to_df(item)
                if not df.empty:
                    all_frames.append(df)

            pagination = payload.get("meta", {}).get("pagination", {})
            page_count = pagination.get("pageCount", 1) or 1
            page_num += 1

        browser.close()

    if not all_frames:
        return pd.DataFrame(columns=COLUMNS)

    df = pd.concat(all_frames, ignore_index=True)
    df = (
        df.drop_duplicates(subset=["Sklad", "Post_Date"], keep="last")
        .sort_values(["Sklad", "Post_Date"])
        .reset_index(drop=True)
    )
    return df


def save_csv(df):
    df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    print()
    print("CSV shranjen:")
    print(CSV_PATH)
    print(f"Skupaj vrstic: {len(df)}")


def main():
    print("=== UNICREDIT START ===")

    df = fetch_unicredit_all()

    if df.empty:
        # Ne prepišemo obstoječega CSV in označimo napako v GitHub Actions
        print("NAPAKA: nobenih podatkov ni bilo pridobljenih.")
        sys.exit(1)

    save_csv(df)
    print("=== UNICREDIT DONE ===")


if __name__ == "__main__":
    main()
