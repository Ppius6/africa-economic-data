"""The indicators to be fetched."""

INDICATORS = {
    # Macro / growth
    "NY.GDP.MKTP.CD": "GDP (current US$)",
    "NY.GDP.PCAP.CD": "GDP per capita (current US$)",
    "NY.GDP.MKTP.KD.ZG": "GDP growth (annual %)",
    "GC.DOD.TOTL.GD.ZS": "Central government debt (% of GDP)",
    "BX.KLT.DINV.WD.GD.ZS": "Foreign direct investment, net inflows (% of GDP)",

    # Trade & external balance
    "NE.EXP.GNFS.ZS": "Exports of goods and services (% of GDP)",
    "NE.IMP.GNFS.ZS": "Imports of goods and services (% of GDP)",
    "BN.CAB.XOKA.GD.ZS": "Current account balance (% of GDP)",

    # Demographics & poverty
    "SP.POP.TOTL": "Population, total",
    "SP.DYN.LE00.IN": "Life expectancy at birth",
    "SP.URB.TOTL.IN.ZS": "Urban population (% of total)",
    "SI.POV.GINI": "GINI index",

    # Labor
    "SL.UEM.TOTL.ZS": "Unemployment, total (% of labor force)",

    # Prices
    "FP.CPI.TOTL.ZG": "Inflation, consumer prices (annual %)",

    # Health
    "SH.DYN.MORT": "Under-5 mortality rate",
    "SH.XPD.CHEX.GD.ZS": "Health expenditure (% of GDP)",
    "SH.STA.MMRT": "Maternal mortality ratio",

    # Education
    "SE.PRM.CMPT.ZS": "Primary completion rate",
    "SE.ADT.LITR.ZS": "Adult literacy rate",

    # Infrastructure & technology
    "IT.NET.USER.ZS": "Individuals using the internet (% of population)",
    "IT.CEL.SETS.P2": "Mobile cellular subscriptions (per 100 people)",
    "EG.ELC.ACCS.ZS": "Access to electricity (% of population)",
    "EG.FEC.RNEW.ZS": "Renewable energy consumption (% of total)",
    "EN.GHG.CO2.PC.CE.AR5": "CO2 emissions per capita",

    # Agriculture
    "NV.AGR.TOTL.ZS": "Agriculture, value added (% of GDP)",
    "AG.LND.AGRI.ZS": "Agricultural land (% of land area)",
}