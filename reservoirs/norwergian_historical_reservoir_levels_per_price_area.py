import requests
import pandas as pd


def norwegian_historical_reservoir_levels_per_price_area():
    # API endpoint
    url = "https://biapi.nve.no/magasinstatistikk/api/Magasinstatistikk/HentOffentligData"

    # Send a GET request
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()  # Parse JSON response
    else:
        print(f"Request failed with status code {response.status_code}")

    df = pd.DataFrame(data)

    df=df[df["omrType"]=="EL"].copy()

    del df["omrType"], df["neste_Publiseringsdato"]

    df=df.rename(columns={"dato_Id":"date","omrnr":"price_area","iso_aar":"iso_year","iso_uke":"iso_week","fyllingsgrad":"reservoir_level", 
                          "kapasitet_TWh":"capacity_twh","fylling_TWh":"reservoir_level_twh","fyllingsgrad_forrige_uke":"reservoir_level_last_week",
                          "endring_fyllingsgrad":"reservoir_level_change"})

    df["price_area"]="NO"+df["price_area"].astype(str)
    df=df.sort_values(by=["price_area","date"], ascending=True).reset_index(drop=True)

    df.to_excel("norwegian_historical_reservoir_levels_per_price_area.xlsx")

if __name__ == '__main__':
    norwegian_historical_reservoir_levels_per_price_area()
