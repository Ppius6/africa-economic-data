import pandas as pd

URL = "https://ourworldindata.org/grapher/world-bank-income-groups.csv?v=1&csvType=full&useColumnShortNames=false"

df = pd.read_csv(URL, storage_options={"User-Agent": "world-economic-data-project/1.0"})
print(df.columns.tolist())
print(df.head(10))