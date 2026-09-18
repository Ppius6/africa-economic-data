import pandas as pd
from datetime import date

URL = "https://ourworldindata.org/grapher/world-bank-income-groups.csv?v=1&csvType=full&useColumnShortNames=false"
SNAPSHOT_CUTOVER = date(2026, 9, 18)

AFRICA_ISO3 = {
    "DZA", "AGO", "BEN", "BWA", "BFA", "BDI", "CPV", "CMR", "CAF", "TCD",
    "COM", "COD", "COG", "CIV", "DJI", "EGY", "GNQ", "ERI", "SWZ", "ETH",
    "GAB", "GMB", "GHA", "GIN", "GNB", "KEN", "LSO", "LBR", "LBY", "MDG",
    "MWI", "MLI", "MRT", "MUS", "MAR", "MOZ", "NAM", "NER", "NGA", "RWA",
    "STP", "SEN", "SYC", "SLE", "SOM", "ZAF", "SSD", "SDN", "TZA", "TGO",
    "TUN", "UGA", "ZMB", "ZWE",
}

VALUE_COLUMN = "World Bank's income classification"

# OWID's labels don't match WDI's own incomeLevel.value strings
# (stg_countries.income_level_name). Normalize so historical (seed) and
# live (snapshot) rows agree on spelling — otherwise the same category
# shows up as two different strings across the unioned dim_country.
LABEL_MAP = {
    "Low-income countries": "Low income",
    "Lower-middle-income countries": "Lower middle income",
    "Upper-middle-income countries": "Upper middle income",
    "High-income countries": "High income",
}

def build_ranges(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["Code"].isin(AFRICA_ISO3)].copy()
    df = df.rename(columns={"Code": "country_code", "Year": "year", VALUE_COLUMN: "income_level_name"})
    df = df.dropna(subset=["income_level_name"])

    unmapped = set(df["income_level_name"].unique()) - set(LABEL_MAP.keys())
    if unmapped:
        print(f"WARNING: Unmapped income labels found, left as-is: {unmapped}")
    df["income_level_name"] = df["income_level_name"].map(LABEL_MAP).fillna(df["income_level_name"])

    df = df.sort_values(["country_code", "year"])

    ranges = []

    for country_code, group in df.groupby("country_code"):
        group = group.reset_index(drop=True)
        range_start_year = group.loc[0, "year"]
        current_value = group.loc[0, "income_level_name"]

        for i in range(1, len(group)):
            row_value = group.loc[i, "income_level_name"]
            row_year = group.loc[i, "year"]
            if row_value != current_value:
                ranges.append({
                    "country_code": country_code,
                    "income_level_name": current_value,
                    "valid_from": date(range_start_year, 7, 1),
                    "valid_to": date(row_year, 7, 1)
                })
                range_start_year = row_year
                current_value = row_value

        ranges.append({
            "country_code": country_code,
            "income_level_name": current_value,
            "valid_from": date(range_start_year, 7, 1),
            "valid_to": SNAPSHOT_CUTOVER,
        })

    return pd.DataFrame(ranges)

def run():
    df = pd.read_csv(URL, storage_options={"User-Agent": "world-economic-data-project/1.0"})
    ranges = build_ranges(df)
    output_path = "dbt_project/seeds/income_classification_history.csv"
    ranges.to_csv(output_path, index=False)
    print(f"Wrote {len(ranges)} historical ranges across {ranges['country_code'].nunique()} countries to {output_path}")

if __name__ == "__main__":
    run()
