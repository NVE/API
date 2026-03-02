import pandas as pd
import numpy as np
import pyodbc

'''
Calculate sum of energy equivalents for each subcatchment (delfelt) in the hydropower GIS data. 
This is done by mapping each subcatchment to the hydropower plants downstream of it, and then summing the energy equivalents of those plants.
'''

def get_delfelt():
    #Run hydropower_gis_subcatchment.py to get excel file
    df=pd.read_excel("hydropower_gis_subcatchment.xlsx")
    df=df[["delfeltNr", "vannkraftverkNr", "oppstromDelfeltListe"]]
    return df

def finn_alle_delfelt(col, delfelt):

    # Gå gjennom en og en rad og lage topomapping mag -> delfelt + oppstrøms
    topo = []

    for i, row in delfelt.iterrows():

        #kun hvis magasin/kv id er satt
        if row[col] > 0:
            #legge til delfelt selv, for sikkerhetes skyld
            topo.append([int(row[col]), int(row['delfeltNr'])])

            #legge til oppstrøms delfeltlist
            opp = row['oppstromDelfeltListe']
            if (opp is not None):
                if isinstance(opp, float) and np.isnan(opp):
                    continue  
                else:   
                     opp = opp.split(',')

                for o in opp:
                    topo.append([int(row[col]), int(o)])

                #det blir en del duplikater av dette, viktig med drop duplicates etterpå.

    topo = pd.DataFrame(data=topo, columns=[col, 'delfeltNr'])
    topo.drop_duplicates(inplace=True)
    return topo

def get_topo_utbygd():
    #run hydropower_gis_downstream_plants.py to get excel file
    df=pd.read_excel("hydropower_gis_downstream_plants.xlsx")
    df=df[["vannkraftverknr","nedstromvannkraftverknr_liste"]].rename(columns={"vannkraftverknr":"vannkraftverkNr"})


    df["nedstromvannkraftverknr_liste"] = df["nedstromvannkraftverknr_liste"].where(df["nedstromvannkraftverknr_liste"].notna(), None)          # behold NaN som NaN/None
    df["nedstromvannkraftverknr_liste"] = df["nedstromvannkraftverknr_liste"].astype("string")                      # pandas string-dtype
    df["nedstromvannkraftverknr_liste"] = df["nedstromvannkraftverknr_liste"].str.split(",")
    df = df.explode("nedstromvannkraftverknr_liste", ignore_index=True)

    df.to_excel("topo_utbygd.xlsx", index=False)
    return df

def legg_til_utbygd_prodvanntildelfelt(topo_kv, topo_utbygd):

    #skanne meg oppover i vannkrafttopologien for å være sikker på at jeg får med meg alle delfeltene

    topo_kv = topo_kv.merge(topo_utbygd, on='vannkraftverkNr')

    del topo_kv['vannkraftverkNr']
    topo_kv.rename(columns={'nedstromvannkraftverknr_liste':'vannkraftverkNr'}, inplace=True)

    #Nå blir det masse duplikater, det er meningen og så tar vi de bort
    topo_kv.drop_duplicates(inplace=True)

    return(topo_kv)

def get_kraftverk():
    import requests

    url = "https://api.nve.no/web/Powerplant/GetHydroPowerPlantsInOperation"

    # Make the request, return data
    response = requests.get(url)
    data = response.json()

    #convert to pandas dataframe, write to Excel
    df=pd.DataFrame(data)

    df=df.rename(columns={"VannKraftverkID": "vannkraftverkNr", "Navn": "vannkraftverkNavn", "maksytelse": "maksytelse", "midprod_81_10": "midprod_81_10", "enekv": "enekv"})

    return df


def hydropower_gis_subcatchment_sum_energy_equivalents():
    delfelt = get_delfelt()

    print(delfelt)

    topo_kv = finn_alle_delfelt('vannkraftverkNr', delfelt)

    print(topo_kv)

    topo_utbygd = get_topo_utbygd()
    
    #dette gjøres for å komme forbi brutte delfeltkoblinger og for kraftverk som er koblet direkte til forrige uten
    # delfelt i mellom
    topo_kv = legg_til_utbygd_prodvanntildelfelt(topo_kv, topo_utbygd )
    print(topo_kv)
    
    vk = get_kraftverk()
    print(vk)
    vk = vk[['vannkraftverkNr', 'vannkraftverkNavn', 'MaksYtelse',"MidProd_91_20","EnEkv"]]

    #finne sum energiekvivalent per delfelt
    vk["vannkraftverkNr"] = vk["vannkraftverkNr"].astype(str)
    topo_kv["vannkraftverkNr"] = topo_kv["vannkraftverkNr"].astype(str)
    enekv_sum=topo_kv.merge(vk[['vannkraftverkNr','EnEkv']], on="vannkraftverkNr")

    enekv_sum=enekv_sum.pivot_table(index="delfeltNr", values="EnEkv", aggfunc="sum").reset_index()
    enekv_sum.to_excel('hydropower_gis_subcatchment_sum_energy_equivalents.xlsx', index=False)
    
if __name__ == '__main__':
    hydropower_gis_subcatchment_sum_energy_equivalents()
