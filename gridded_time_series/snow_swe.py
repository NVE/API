import requests
import pandas as pd


'''
swe= snow water equivalent,  representing the depth of water that would result if the snow melted instantly
'''

def get_body(StartDate, EndDate, Method, Rings):
    body={
    "Theme": "swe",
    "StartDate": StartDate,
    "EndDate": EndDate,
    "Format": "json",
    "Method": Method,# mean or sum
    "Rings": f"{{'rings': {Rings}, 'spatialReference': {{'wkid': 25833}}}}"
    }
    return body
url='https://gts.nve.no/api/AggregationTimeSeries/ByGeoJson'


body=get_body(StartDate="2024-01-01", EndDate="2024-01-03", Method="avg", Rings=[[[270915,6664551],[270872,6664362],[268633,6664858],[268682,6665047],[270405,6664796],[270518,6664739],[270915,6664551]]])
print(body)

response = requests.post(url, json=body)
data=response.json()

df=pd.DataFrame(data)
print(df)
df.to_excel(f"snow_swe.xlsx", index=False)
