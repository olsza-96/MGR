from pymongo import MongoClient
from pymongo.errors import OperationFailure
import logging as log
import time
import ssl
from scipy.spatial import ConvexHull
from pyproj import Geod
import numpy as np


log.getLogger().setLevel(log.INFO)
log.basicConfig(format="%(asctime)s - [%(levelname)s]: %(message)s", datefmt="%H:%M:%S")

def get_buildable_nodes(region_id: int):
    start = time.time()
    log.info(f"Connecting to the database")

    connection = MongoClient("mongodb+srv://olga:MGR12345%21@sandbox.iseuv.mongodb.net/Poland_spatial_data?retryWrites=true&w=majority", authSource = "admin",  ssl_cert_reqs=ssl.CERT_NONE)

    try:
        connection.server_info()
        log.info(f"Connected successfully")
    except OperationFailure:
        log.error(f"Could not connect to db")

    db = connection.Poland_spatial_data
    current_collection = db["testing_col"]

    query = [
        {
            '$match': {
                'region_id': 1,
                'landuse': {
                    '$in': [
                        'farmland', 'meadow', 'brownfield', 'orchard', 'grass'
                    ]
                },
                'is_buildeable': True
            }
        }, {
            '$group': {
                '_id': '$way_id',
                'buildable_nodes': {
                    '$addToSet': '$id'
                },
                'node_coordinates': {
                    '$addToSet': '$coordinates'
                }
            }
        }
    ]

    cur = current_collection.aggregate(pipeline=query)

    if cur != None:
        allowable_nodes = list(cur)

        overall_buildeable_area, overall_allowable_power = 0, 0
        for way in allowable_nodes:
            log.info(f"Calculating for way: {way['_id']}")
            buildable_area_way, allowable_power_way = calculate_way_area(way)
            overall_buildeable_area = overall_buildeable_area + buildable_area_way
            overall_allowable_power = overall_allowable_power + allowable_power_way



        current_collection = db["regions"]
        current_collection.update_one({"id": region_id}, {"$set": {"results_500m": {"overall_area":overall_buildeable_area,
                                                                "overall_power": overall_allowable_power}}}
                              , upsert= False)

        log.info(f"Data of buildable areas and power inserted to region {region_id}")
        time.sleep(1)
        end = time.time()
        log.info(f"Process of calculating data for region {region_id} took {end - start} seconds")
    else:
        log.info(f"No buildable ways for region {region_id}")
        pass

def calculate_way_area(way: dict):
    # specify a named ellipsoid
    geod = Geod(ellps="WGS84")
    if len(way["buildable_nodes"]) > 2:
        points = np.array(way["node_coordinates"])
        #find vertices of shape
        hull = ConvexHull(points)
        hull_indices = np.unique(hull.simplices.flat)
        shape_vertices = points[hull_indices, :]

        longitudes = [x[0] for x in points]
        latitudes = [x[1] for x in points]
        #returns area in meters squared
        area = abs(geod.polygon_area_perimeter(lons=longitudes, lats=latitudes)[0])

        #log.info(f"Area for the way: {way['_id']} is {area} m2")
        buildable_area = area*1e-06 #conversion to km2

        avg_power_coefficient = 19.8 # MW/ km2 power density coefficient

        allowable_power = buildable_area * avg_power_coefficient

        return buildable_area, allowable_power
    else:
        return 0,0

if __name__ == "__main__":
    for i in range(1, 381):
        get_buildable_nodes(i)