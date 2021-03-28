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

def get_buildable_nodes(region_id: int, min_allowable_power: int):
    """
    :param region_id:
    :param min_allowable_power: minimal power limit for one area, MW
    """
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
    #delete some field from document
    #db["regions"].update_one({}, {"$unset": {"results_500": 1}})

    query = [
        {
            '$match': {
                'region_id': region_id,
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
                },
                'node_distances': {
                    '$addToSet': '$closest_distance_restriction'
                }
            }
        }
    ]

    cur = current_collection.aggregate(pipeline=query)
    allowable_nodes = list(cur)

    if len(allowable_nodes) != 0:
        process_nodes_for_distance(region_id, 0.5,allowable_nodes, db, min_allowable_power)
        process_nodes_for_distance(region_id, 0.75,allowable_nodes, db, min_allowable_power)
        process_nodes_for_distance(region_id, 1.,allowable_nodes, db, min_allowable_power)
        process_nodes_for_distance(region_id, 1.25,allowable_nodes, db, min_allowable_power)
        process_nodes_for_distance(region_id, 1.5,allowable_nodes, db, min_allowable_power)

        time.sleep(1)
        end = time.time()
        log.info(f"Process of calculating data for region {region_id} took {end - start} seconds")

    else:
        log.info(f"No buildable ways for region {region_id}")
        pass

def process_nodes_for_distance(region_id: int, min_distance: float, allowable_nodes: list, db, min_allowable_power: int):
    filtered_ways = filter_data_distance(allowable_nodes, min_distance)
    overall_buildeable_area, overall_allowable_power, node_number = iterate_allowable_ways(filtered_ways, min_allowable_power)
    update_collection(db, region_id, overall_buildeable_area, overall_allowable_power, node_number, min_distance, min_allowable_power)

def filter_data_distance(data: list, min_distance: float):
    filtered_data = []
    for way in data:
        filtered_way = {}
        filtered_indices = [index for index, item in enumerate(way["node_distances"]) if item >= min_distance]
        filtered_way["_id"] = way["_id"]
        filtered_way["buildable_nodes"] = [element for index, element in enumerate(way["buildable_nodes"]) if index in filtered_indices]
        filtered_way["node_coordinates"] = [element for index, element in enumerate(way["node_coordinates"]) if index in filtered_indices]
        filtered_way["node_distances"] = [element for index, element in enumerate(way["node_distances"]) if index in filtered_indices]
        if len(filtered_way["buildable_nodes"]) != 0:
            filtered_data.append(filtered_way)
    if len(filtered_data) != 0:
        return filtered_data
    else:
        return 0

def iterate_allowable_ways(allowable_nodes: list, min_allowable_power: int):

    overall_buildeable_area, overall_allowable_power, number_nodes = 0, 0, 0
    if type(allowable_nodes)!= int:
        for way in allowable_nodes:
            log.info(f"Calculating for way: {way['_id']}")
            buildable_area_way, allowable_power_way = calculate_way_area(way, min_allowable_power)
            overall_buildeable_area = overall_buildeable_area + buildable_area_way
            overall_allowable_power = overall_allowable_power + allowable_power_way

            number_nodes = number_nodes + len(way["buildable_nodes"])
    else:
        pass

    return overall_buildeable_area, overall_allowable_power, number_nodes

def calculate_way_area(way: dict, min_allowable_power: int):
    # specify a named ellipsoid
    geod = Geod(ellps="WGS84")
    if len(way["buildable_nodes"]) > 2:
        points = np.array(way["node_coordinates"])
        #find vertices of shape
        hull = ConvexHull(points)
        hull_indices = np.unique(hull.simplices.flat)
        shape_vertices = points[hull_indices, :]

        longitudes = [x[0] for x in shape_vertices]
        latitudes = [x[1] for x in shape_vertices]
        #returns area in meters squared
        area = abs(geod.polygon_area_perimeter(lons=longitudes, lats=latitudes)[0])

        #log.info(f"Area for the way: {way['_id']} is {area} m2")
        buildable_area = area*1e-06 #conversion to km2

        avg_power_coefficient = 19.8 # MW/ km2 power density coefficient

        allowable_power = buildable_area * avg_power_coefficient
        if allowable_power >= min_allowable_power:
            return buildable_area, allowable_power
        else:
            return 0,0
    else:
        return 0,0

def update_collection(db, region_id: int, overall_buildeable_area: float,
                      overall_allowable_power: float, node_number: int, distance: float, min_allowable_power: int):
    distance = int(distance*1e3)
    current_collection = db["regions"]
    current_collection.update_one({"id": region_id}, {"$set": {f"results_{distance}m_min{min_allowable_power}MW": {"overall_area":overall_buildeable_area,
                                                                                        "overall_power": overall_allowable_power,
                                                                                        "node_number": node_number}}}
                                  , upsert= False)

    log.info(f"Data of buildable areas and power inserted to region {region_id}")



if __name__ == "__main__":

    for i in range(1,381):
        get_buildable_nodes(i, 30)
        