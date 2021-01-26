from pymongo import MongoClient
from pymongo.errors import OperationFailure
import math
import logging as log
import time


log.getLogger().setLevel(log.INFO)
log.basicConfig(format="%(asctime)s - [%(levelname)s]: %(message)s", datefmt="%H:%M:%S")

def get_nodes_from_way(host: str, port: int):
    log.info(f"Connecting to the database")
    connection = MongoClient(host, port)
    try:
        connection.server_info()
        log.info(f"Connected successfully")
    except OperationFailure:
        log.error(f"Could not connect to db")

    db = connection.Poland_spatial_data
    allowable_landuse = ["farmland", "meadow", "brownfield", "orchard", "grass"]
    restricted_landue = ["residential", "nature_reserve","construction","military"]
    attributes = {"nodes": 1, "_id": 0}

    allowable_nodes = query_get_nodes_from_way(allowable_landuse, attributes, db.ways)
    restricting_nodes = query_get_nodes_from_way(restricted_landue, attributes, db.ways)
    iterate_nodes_list(allowable_nodes, restricting_nodes, db)

def query_get_nodes_from_way(landuse: list, attributes: dict, col):
    cursor = col.find({"landuse": {"$in": landuse}}, attributes)
    nodes_list: list = []
    for doc in cursor:
        if not nodes_list:
            nodes_list = doc["nodes"]
        else:
            nodes_list = nodes_list + doc["nodes"]

    return nodes_list

def iterate_nodes_list(nodes_allowable: list, nodes_res:list, database):
    for node in nodes_allowable:
        log.info(f"Looking for closest neighbour for node: {node}")
        start = time.time()
        node_final = find_closest_restriction(node, database.nodes, nodes_res)
        insert_to_collection(node_final, database.nodes_final)
        time.sleep(1)
        end = time.time()
        log.info(f"Process took {end - start} seconds")


def find_closest_restriction(node_id: int, collection, restricting_nodes: list):

    cursor_current_node = collection.find({"id": node_id}, {"_id": 0, "region_id": 0})
    node = next(cursor_current_node, None)
    min_allowable_distance: float = 0.5
    closest_distance: float = 0

    final_res_node_id: int = 0
    for res_node_id in restricting_nodes:
        restricting_node = next(collection.find({"id": res_node_id}, {"_id": 0, "region_id": 0}), None)
        current_calculated_distance = calculate_distance(node, restricting_node)

        if closest_distance == 0:
            closest_distance = current_calculated_distance

        if current_calculated_distance < min_allowable_distance:
            closest_distance = 0
            break
        elif (closest_distance > current_calculated_distance):
            closest_distance = current_calculated_distance
            final_res_node_id = res_node_id
    node["closest_distance_restriction"] = closest_distance
    node["restricting_node_id"] = final_res_node_id
    if closest_distance >= min_allowable_distance:
        node["is_buildeable"] = True
    else:
        node["is_buildeable"] = False

    return node

def insert_to_collection(document: dict, collection):

    cursor_check = collection.find({"id": document["id"]})
    if cursor_check.count() == 0:
        log.info("Cursor is empty")
        collection.insert_one(document)
    else:
        log.info(f"The cursor for {document['id']} already exists")

    #collection.insert_one(document)

def calculate_distance(node1, node2):
    """Calculates Haversine distance in kilometers between two nodes
    :param node1, node2:
    :return distance:
    """
    lat1, lon1 = node1['lat'], node1['lon']
    lat2, lon2 = node2['lat'], node2['lon']
    radius = 6371  # km

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) * math.sin(dlat / 2) +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) * math.sin(dlon / 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = radius * c

    return distance


if __name__ == "__main__":
    get_nodes_from_way('localhost', 27017)