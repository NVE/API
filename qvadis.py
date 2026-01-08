import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, MultiPoint  # Importing Point and MultiPoint classes

'''
Gir reguleringsgrad i % andel av normal årsavrenning
'''


def qvadis():
    # URL of the service, WMS
    url = 'https://kart.nve.no/enterprise/rest/services/Mapservices/Qvadis/MapServer/1/query'

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
        'outFields': '*', #* means returning all columns. YOu can replace this this with a list of columns out want to return.
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
    print(attributes)
    geometry_paths = [feature['geometry']['paths'] for feature in all_features]
    # Extract points from rings for each row
    geometry_points = [MultiPoint([Point(point) for point in path]) for paths in geometry_paths for path in paths]
    print(len(geometry_points))
    # Create DataFrame
    df = pd.DataFrame(attributes)
    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry=geometry_points)

    print(gdf)  # Print the GeoDataFrame
    print(gdf.columns)

    gdf.to_excel("qvadis.xlsx", index=False)

if __name__ == '__main__':
    qvadis()
