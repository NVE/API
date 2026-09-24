
import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import pyodbc
import requests
from shapely.geometry import Point, MultiPoint, Polygon  # Importing Point and MultiPoint classes

# Function to fetch data with pagination
def fetch_data_with_pagination(url, params, batch_size=1000):
    all_features = []
    offset = 0
    while True:
        params['resultOffset'] = offset
        response = requests.get(url, params=params)
        if response.status_code == 200:
            json_data = response.json()
            features = json_data.get('features', [])
            if not features:
                break
            all_features.extend(features)
            if len(features) < batch_size:
                break
            offset += batch_size
        else:
            print("Error:", response.status_code)
            break
    return all_features

params = {
        'where': '1=1',  # True
        'text': '',
        'objectIds': '',
        'time': '',
        'timeRelation': 'esriTimeRelationOverlaps',
        'geometry': '',
        'geometryType': 'esriGeometryEnvelope',
        'inSR': '',
        'spatialRel': 'esriSpatialRelIntersects',
        'distance': '',
        'units': 'esriSRUnit_Meters',
        'relationParam': '',
        'outFields': '*', #* means returning all columns. You can replace this this with a list of columns out want to return.
        'returnGeometry': True,
        'returnTrueCurves': False,
        'maxAllowableOffset': '',
        'geometryPrecision': '',
        'outSR': '',
        'havingClause': '',
        'returnIdsOnly': False,
        'returnCountOnly': False,
        'orderByFields': '',
        'groupByFieldsForStatistics': '',
        'outStatistics': '',
        'returnZ': False,
        'returnM': False,
        'gdbVersion': '',
        'historicMoment': '',
        'returnDistinctValues': False,
        'resultOffset': '',
        'resultRecordCount': 1000,  # Set batch size to 1000
        'returnExtentOnly': False,
        'sqlFormat': 'none',
        'datumTransformation': '',
        'parameterValues': '',
        'rangeValues': '',
        'quantizationParameters': '',
        'featureEncoding': 'esriDefault',
        'f': 'pjson'
    }

def hydropower_gis_subcatchment():
    # URL of the service, WMS
    url = 'https://gis3.nve.no/map/rest/services/Mapservices/VassdragsreguleringVannkraft/MapServer/8/query'

    # Parameters for the request
    
    # Fetch all features using pagination
    all_features = fetch_data_with_pagination(url, params)

    # Extracting attributes and geometry coordinates
    filtered_features = [feature for feature in all_features 
                        if 'attributes' in feature and 'geometry' in feature]

    attributes = []
    geometry_polygons = []

    for feature in filtered_features:
        if 'attributes' in feature and 'geometry' in feature:
            attributes.append(feature['attributes'])
            
            # ESRI returns geometry["rings"] → list of rings (outer + holes)
            rings = feature['geometry']['rings']
            
            # Create Polygon directly from rings
            polygon = Polygon(rings[0], rings[1:]) if len(rings) > 1 else Polygon(rings[0])
            
            geometry_polygons.append(polygon)

    # Create DataFrame
    df = pd.DataFrame(attributes)

    # Ensure lengths match before making GeoDataFrame
    if len(df) == len(geometry_polygons):
        gdf = gpd.GeoDataFrame(df, geometry=geometry_polygons, crs="EPSG:25833")
    else:
        print("Mismatch in lengths of attributes and geometry entries")

    print(gdf)
    print(gdf.columns)

    return gdf


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
    # URL of the service, WMS
    url = 'https://kart.nve.no/enterprise/rest/services/Vannkraft1/MapServer/0/query'


    # Parameters for the request
    params = {
        'where': '1=1',  # True
        'text': '',
        'objectIds': '',
        'time': '',
        'timeRelation': 'esriTimeRelationOverlaps',
        'geometry': '',
        'geometryType': 'esriGeometryEnvelope',
        'inSR': '',
        'spatialRel': 'esriSpatialRelIntersects',
        'distance': '',
        'units': 'esriSRUnit_Meters',
        'relationParam': '',
        'outFields': 'vannkraftverknr,nedstromvannkraftverknr_liste', #* means returning all parameters. You can replace this this with a list of columns out want to return.
        'returnGeometry': True,
        'returnTrueCurves': False,
        'maxAllowableOffset': '',
        'geometryPrecision': '',
        'outSR': '',
        'havingClause': '',
        'returnIdsOnly': False,
        'returnCountOnly': False,
        'orderByFields': '',
        'groupByFieldsForStatistics': '',
        'outStatistics': '',
        'returnZ': False,
        'returnM': False,
        'gdbVersion': '',
        'historicMoment': '',
        'returnDistinctValues': False,
        'resultOffset': '',
        'resultRecordCount': 1000,  # Set batch size to 1000
        'returnExtentOnly': False,
        'sqlFormat': 'none',
        'datumTransformation': '',
        'parameterValues': '',
        'rangeValues': '',
        'quantizationParameters': '',
        'featureEncoding': 'esriDefault',
        'f': 'pjson'
    }

    # Fetch all features using pagination
    all_features = fetch_data_with_pagination(url, params)

    # Extracting attributes and geometry coordinates
    attributes = [feature['attributes'] for feature in all_features]
    # geometry_x = [feature['geometry']['x'] for feature in all_features]
    # geometry_y = [feature['geometry']['y'] for feature in all_features]

    # Create DataFrame
    df = pd.DataFrame(attributes)

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
    # enekv_sum.to_excel('hydropower_gis_subcatchment_sum_energy_equivalents.xlsx', index=False)
    return enekv_sum


def get_norge_flate():
    #Hente landområder for Norge, brukes får å fjerne hav senere
    # URL of the service, WMS
    url = 'https://gis3.nve.no/map/rest/services/Mapservices/Administrasjon/MapServer/11/query'


    # Fetch all features using pagination
    all_features = fetch_data_with_pagination(url, params)

    # Extracting attributes and geometry coordinates
    filtered_features = [feature for feature in all_features 
                        if 'attributes' in feature and 'geometry' in feature]

    attributes = []
    geometry_polygons = []

    for feature in filtered_features:
        if 'attributes' in feature and 'geometry' in feature:
            attributes.append(feature['attributes'])
            
            # ESRI returns geometry["rings"] → list of rings (outer + holes)
            rings = feature['geometry']['rings']
            
            # Create Polygon directly from rings
            polygon = Polygon(rings[0], rings[1:]) if len(rings) > 1 else Polygon(rings[0])
            
            geometry_polygons.append(polygon)

    # Create DataFrame
    df = pd.DataFrame(attributes)

    # Ensure lengths match before making GeoDataFrame
    if len(df) == len(geometry_polygons):
        gdf = gpd.GeoDataFrame(df, geometry=geometry_polygons, crs="EPSG:25833")
    else:
        print("Mismatch in lengths of attributes and geometry entries")

    print(gdf)
    print(gdf.columns)
    print(gdf["OBJTYPE"].unique())
    gdf=gdf[gdf["OBJTYPE"]!="Havflate"]

    return gdf



sum_eneq=hydropower_gis_subcatchment_sum_energy_equivalents()
delfelt=hydropower_gis_subcatchment()
delfelt=delfelt.merge(sum_eneq, on="delfeltNr", how="outer")
norge_flate = get_norge_flate()

#gjøre om til 1 polygon
norge_flate['dummy'] = 1
norge_flate = norge_flate.dissolve(by='dummy')
norge_flate = norge_flate.reset_index()
del norge_flate['dummy']
print(norge_flate)


fig, ax = plt.subplots(figsize=(20,20))

norge_flate.plot(ax=ax, color='w', edgecolor='k')
#RdYlGn
delfelt.plot(ax=ax, column='EnEkv', legend=True, cmap='Blues',  legend_kwds={'label': "Energiekvivalent til delfelt ($kWh/m^{3}$)",
                        'orientation': "vertical"}, vmax =3);  #viridis_r, Blues, Purples, BuPu, cool

# GeoPandas passes legend_kwds to Matplotlib Colorbar; fontsize must be set on the colorbar axis
if len(fig.axes) > 1:
    cbar_ax = fig.axes[-1]
    cbar_ax.tick_params(labelsize=16)
    cbar_ax.set_ylabel("Energiekvivalent til delfelt ($kWh/m^{3}$)", fontsize=20)
props = dict(boxstyle='round', facecolor='w')
ax.axis('off')
ax.set_title("Hvor er nedbøren mest verdt?", fontsize=30)
# place a text box in upper left in axes coords
ax.text(0.05, 0.95, " Kartet viser nedbørsfeltene til vannkraftverkene i \n Norge. Nedbørsfeltene er farget etter hvor mye energi \n man får fra hver dråpe nedbør gjennom kraftverkene på \n vei ned til havet. En sumenergiekvivalent på 1 betyr at \n man får 1 kWh med strøm for 1 m3 vann. Dette er \n direkte proporsjonalt med den utnyttede fallhøyden og \n virkningsgraden til kraftverkene i kaskaden. En \n energiekvivalent på 1 tilsvarer et fall på omtrent 400 m.", transform=ax.transAxes, fontsize=14,
        verticalalignment='top', bbox=props)
fig.savefig('hvor_er_nedbøren_mest_verdt_blå.png')
