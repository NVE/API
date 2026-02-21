import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, MultiPoint  # Importing Point and MultiPoint classes

'''
Columns:
OBJECTID, objektType, delfeltNr, delfeltNavn, delfeltAreal_km2, vannkraftverkNr, vannkraftverkNavn, magasinNr, magasinNavn, 
nesteDelfeltNr, delfeltFormal, vassdragsomradeNr, oppstromDelfeltListe, QNormalDelfelt6190_Mm3Aar, QNormalDelfelt9120_Mm3Aar, geometry
'''


def hydropower_subcatchments_gis():
    # URL of the service, WMS
    url = 'https://gis3.nve.no/map/rest/services/Mapservices/VassdragsreguleringVannkraft/MapServer/8/query'

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

    # Fetch all features using pagination
    all_features = fetch_data_with_pagination(url, params)

    # Extracting attributes and geometry coordinates
    filtered_features = [feature for feature in all_features if 'attributes' in feature and 'geometry' in feature]
    attributes = []
    geometry_points = []

    for feature in filtered_features:
        if 'attributes' in feature and 'geometry' in feature:
            attributes.append(feature['attributes'])
            rings = feature['geometry']['rings']
            points = [Point(point) for ring in rings for point in ring]
            geometry_points.append(MultiPoint(points))

    # Create DataFrame
    df = pd.DataFrame(attributes)

    # Ensure the lengths match before creating GeoDataFrame
    if len(df) == len(geometry_points):
        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame(df, geometry=geometry_points)
    else:
        print("Mismatch in lengths of attributes and geometry points")

    print(gdf)  # Print the GeoDataFrame
    print(gdf.columns)

    gdf.to_excel("hydropower_subcatchments_gis.xlsx", index=False)

if __name__ == '__main__':
    hydropower_subcatchments_gis()
