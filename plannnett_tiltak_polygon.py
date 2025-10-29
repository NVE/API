import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, MultiPoint, Polygon  


def plannett_tiltak_polygon():
    # URL of the service, WMS
    url = 'https://gis3.nve.no/map/rest/services/Mapservices/PlanNettv2/MapServer/4/query'

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
    geometry_polygon = []

    for feature in filtered_features:
        if 'attributes' in feature and 'geometry' in feature:
            attributes.append(feature['attributes'])
            rings = feature['geometry']['rings']
            polygon = Polygon(rings[0]) if rings else None
            geometry_polygon.append(polygon)

    # Create DataFrame
    df = pd.DataFrame(attributes)

    # Ensure the lengths match before creating GeoDataFrame
    if len(df) == len(geometry_polygon):
        # Create GeoDataFrame
        gdf = gpd.GeoDataFrame(df, geometry=geometry_polygon)
    else:
        print("Mismatch in lengths of attributes and geometry points")

    print(gdf)  # Print the GeoDataFrame
    print(gdf.columns)

    gdf.to_excel("plannett_tiltak_polygon.xlsx", index=False)

if __name__ == '__main__':
    plannett_tiltak_polygon()