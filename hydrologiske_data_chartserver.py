import pandas as pd
import requests
from multiprocessing import Pool
import io
import yaml
from datetime import timedelta

def les_parametere():
    df = pd.DataFrame(
        [
            ["Tilsig ", 1050, 51],
            ["Mod lufttemperatur (°C)", 6000, 3],
            ["Modellert nedbør (m)", 6001,3],
            ["Modellert vannføring (m³/s)", 6004, 3],
            ["Mod snøens vannekvivalent (m)", 6005,3], #snømagasin
            ["Mod markvannsinnhold (m)", 6013, 3],
            ["Mod grunnvannsvolum",6018, 3], 
            #["Mod sum fordampning fra sjø", 6002, 3],
            #["Mod snødekningsgrad (%)", 6008, 3],
            #["Mod grunnvannsvolum",6017, 3], # ?
        ],
        columns = ["Parameter","parameterkode","versjon"]
    )
    return df

def serier(vannmerke_ID, serienavn, start_time, stop_time):
    start_time=start_time.replace('-', '')+"T0000"
    stop_time=stop_time.replace('-', '')+"T0000"
    #rt=1:00 er time, rt=1 er døgn
    rt="1" # day resolution
    da = 37 #HYDAG_POINT
    mth="mean" #snittverdier per døgn. Alternativet er "inst" som angir momentanverdier

    url = f"https://chartserver.nve.no/ShowData.aspx?req=getchart&ver=1.0&&time={start_time};{stop_time}&lang=no&chlf=desc&chsl=0;+0&chd=ds=htsr,da={da},id={vannmerke_ID},rt={rt},cht=line,mth={mth},clr=black,drwd=2&vfmt=text"
    print(url)
    r = requests.get(url).text
    serie = proc_results(r, vannmerke_ID, serienavn)
    return serie

def proc_results(r, vannmerke_ID, serienavn):
    r2 = r.replace('<br />','\n')
    q = pd.read_csv(io.StringIO(r2), sep=', ', decimal=',', engine='python')
    q=q.iloc[:, :2]
    q.columns=['datetime','q']
    q['datetime'] = q['datetime'].str.strip(',')
    q['dato'] = pd.to_datetime(q['datetime'], format='%d.%m.%Y %H:%M:%S', errors='coerce')
    assert q[q['q'].isnull()].empty, print(q[q['q'].isnull()])
    assert len(q["q"] > 0), print("ingen observasjoner")
    serie = q[['dato', 'q']].copy()
    serie.rename(columns={ 'q': 'verdi'}, inplace=True)
    serie["vannmerke_ID"] = vannmerke_ID
    serie["serienavn"] = serienavn
    return serie

def process_row(args):
    params_row, df_row, start_time, stop_time = args
    temp = serier(vannmerke_ID=df_row["maalestasjon"] + ".0." + str(params_row["parameterkode"]) + "." + str(int(params_row["versjon"])),
                            serienavn=df_row["serienavn"], start_time=start_time, stop_time=stop_time)
    temp["parameter"] = params_row["Parameter"]
    return temp

def laste_ned_serier_fra_hydra(df, workers):

    start_time = ("1958-01-01")
    stop_time = ("2024-01-01")

    params = les_parametere()
    with Pool(workers) as pool:
        rows_params_combinations = [(p, row, start_time, stop_time) for _, p in params.iterrows() for _, row in df.iterrows()]
        results = pool.map(process_row, rows_params_combinations)
    return pd.concat(results).reset_index(drop=True)

if __name__ == "__main__":
    df_stations = pd.read_csv("hydrologiske_data_chartserver_stations.csv", dtype={"serienavn": str, "maalestasjon": str})
    # note: only do a subset of the stations for testing. Try df_stations[0:5] in the line below
    df_all = laste_ned_serier_fra_hydra(df_stations,workers=10)  
    df_all.to_csv("hydrologiske_data_chartserver.csv")
    print(df_all)
