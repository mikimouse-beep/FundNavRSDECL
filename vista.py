import os

import pandas as pd
from playwright.sync_api import sync_playwright


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "vista.csv")


VISTA_URLS = {
    "VISTA RICA CORPORATE": "https://vistarica.rs/dnevni-izvestaji-vista-rica-corporate/",
    "VISTA RICA INVEST": "https://vistarica.rs/dnevni-izvestaji-vista-rica-invest/",
    "VISTA CASH": "https://vistarica.rs/dnevni-izvestaji-vista-cash/",
    "VISTA EURO CASH": "https://vistarica.rs/dnevni-izvestaji-vista-euro-cash/",
    "VISTA RICA ORIGIN": "https://vistarica.rs/dnevni-izvestaji-vista-rica-origin/",
}


def parse_eu_decimal(value):
    if pd.isna(value):
        return None

    value = str(value).strip()

    if not value:
        return None

    value = value.replace(".", "").replace(",", ".")

    try:
        return float(value)
    except ValueError:
        return None


def fetch_vista_all_pages(url, sklad):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print(f"\n--- START {sklad} ---")
        print(f"Odpiram: {url}")

        page.goto(
            url,
            wait_until="networkidle",
            timeout=60000,
        )

        page.wait_for_selector(
            "table.wpDataTable",
            timeout=30000,
        )

        header_elements = page.query_selector_all(
            "table.wpDataTable thead th"
        )

        header = [
            th.inner_text().strip()
            for th in header_elements
        ]

        def read_rows():
            rows = page.query_selector_all(
                "table.wpDataTable tbody tr"
            )

            out = []

            for row in rows:
                cols = row.query_selector_all("td")

                values = [
                    col.inner_text().strip()
                    for col in cols
                ]

                if values:
                    out.append(values)

            return out

        all_rows = read_rows()

        # Paginacija s številčnimi gumbi
        while True:
            buttons = page.query_selector_all(
                "a.paginate_button:not(.previous):not(.next)"
            )

            if not buttons:
                break

            current_btn = page.query_selector(
                "a.paginate_button.current"
            )

            current_page = (
                current_btn.inner_text().strip()
                if current_btn
                else "1"
            )

            last_page = buttons[-1].inner_text().strip()

            if current_page == last_page:
                break

            next_index = None

            for i, btn in enumerate(buttons):
                if btn.inner_text().strip() == current_page:
                    next_index = i + 1
                    break

            if next_index is None or next_index >= len(buttons):
                break

            buttons[next_index].click()
            page.wait_for_timeout(600)

            new_rows = read_rows()
            all_rows.extend(new_rows)

        browser.close()

    df = pd.DataFrame(
        all_rows,
        columns=header,
    )

    if df.empty:
        print(f"NAPAKA: {sklad} nima podatkov.")
        return pd.DataFrame()

    df["Sklad"] = sklad

    try:
        price_col = next(
            c
            for c in df.columns
            if "(RSD)" in c
            and "investicione" in c
        )

        amount_col = next(
            c
            for c in df.columns
            if "(RSD)" in c
            and "imovine" in c
        )

    except StopIteration:
        print(
            f"NAPAKA: {sklad}: "
            f"RSD stolpci niso prepoznani."
        )
        print("Header:", df.columns.tolist())
        return pd.DataFrame()

    df["PricePerUnit"] = (
        df[price_col]
        .apply(parse_eu_decimal)
    )

    df["Amount"] = (
        df[amount_col]
        .apply(parse_eu_decimal)
    )

    df["Num"] = (
        df["Amount"]
        / df["PricePerUnit"]
    )

    df["Post_Date"] = pd.to_datetime(
        df["Datum"],
        dayfirst=True,
        errors="coerce",
    )

    df = df[
        [
            "Post_Date",
            "Sklad",
            "Amount",
            "PricePerUnit",
            "Num",
        ]
    ]

    df = df.dropna(
        subset=["Post_Date"]
    )

    df = df.sort_values(
        "Post_Date"
    )

    print(f"Prebranih vrstic: {len(df)}")
    print(f"--- DONE {sklad} ---")

    return df


def fetch_vista_all():
    all_frames = []

    for sklad, url in VISTA_URLS.items():
        try:
            df = fetch_vista_all_pages(
                url,
                sklad,
            )

            if not df.empty:
                all_frames.append(df)

        except Exception as e:
            print(
                f"NAPAKA pri skladu "
                f"{sklad}: {e}"
            )

    if not all_frames:
        return pd.DataFrame()

    return pd.concat(
        all_frames,
        ignore_index=True,
    )


def save_csv(df):
    df.to_csv(
        CSV_PATH,
        index=False,
        encoding="utf-8-sig",
    )

    print()
    print("CSV shranjen:")
    print(CSV_PATH)
    print(f"Skupaj vrstic: {len(df)}")


def main():
    print("=== VISTA START ===")

    df = fetch_vista_all()

    if df.empty:
        print(
            "NAPAKA: nobenih podatkov "
            "ni bilo pridobljenih."
        )
        return

    save_csv(df)

    print("=== VISTA DONE ===")


if __name__ == "__main__":
    main()
