import pandas as pd
import requests

#parametre:
# Parameter	Title	Unit
# 6	Islossning
# 5	Isläggning
# 7	Istjocklek	cm
# 8	Snödensitet	g/cm^3
# 4	Vattendragstemperatur	°C
# 2	Vattenföring (15 min)	m³/s
# 1	Vattenföring (Dygn)	m³/s
# 10	Vattenföring (Månad)	m³/s
# 9	Vatteninnehåll	mm
# 3	Vattenstånd	cm

def inflow_smhi():
    kobling=pd.read_excel("smhi_api_stasjoner.xlsx")

    print(kobling)

    for serie,row in kobling.groupby("Stasjonsnr"):

        # Define the endpoint URL
        #dokumentasjon: https://opendata.smhi.se/hydroobs/introduction
        parameter=1 #Vattenföring (Dygn)	m³/s
        url = f"https://opendata-download-hydroobs.smhi.se/api/version/latest/parameter/{parameter}/station/{serie}/period/corrected-archive/data.json"

        # Make a GET request to the endpoint
        response = requests.get(url)

        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()

            # Extract relevant data
            parameter_name = data['parameter']['name']
            station_name = data['station']['name']
            print(station_name)
            station_ID = data['station']['key']
            emps_kode=row["Seriekode_EMPS"].values[0]

            # Convert the value list to a DataFrame
            df = pd.DataFrame(data['value'])

            # Convert the date from milliseconds to a readable date format
            df['date'] = pd.to_datetime(df['date'], unit='ms')

            # Add parameter name and station name to the DataFrame
            df['parameter_name'] = parameter_name
            df['station_name'] = station_name
            df['station_id'] = station_ID
            df['emps_kode'] = emps_kode

            df.to_csv(f"{station_name}.csv", index=False)

        else:
            print(f"Request failed with status code {response.status_code}")

if __name__ == '__main__':
    inflow_smhi()
