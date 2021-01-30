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
    current_collection = db["ways"]
    allowable_landuse = ["farmland", "meadow", "brownfield", "orchard", "grass"]
    attributes = {"nodes": 1, "_id": 0}
    start = time.time()
    allowable_nodes = query_get_nodes_from_way(allowable_landuse, attributes, current_collection)
    iterate_nodes_list(allowable_nodes, db)
    time.sleep(1)
    end = time.time()
    log.info(f"Process took {(end - start) / 60} minutes")

    start = time.time()
    #restricting_nodes = query_get_nodes_from_way(restricted_landue, attributes, current_collection)
    time.sleep(1)
    end = time.time()
    log.info(f"Process took {(end - start) / 60} minutes")


def get_region_ids(node_id: int, database):
    cursor = database["nodes"].find({"id": node_id}, {"_id": 0, "region_id": 1})
    region_id = next(cursor, None)["region_id"]
    cursor_reg = database["regions"].find({"id": region_id}, {"_id": 0, "id": 1, "neighbours": 1})
    regions_to_extract = []
    for doc in cursor_reg:
        regions_to_extract.append(doc["id"])
        regions_to_extract.extend(doc["neighbours"])

    return regions_to_extract

def get_restricting_nodes(region_id, landuse_types:list, database):
    log.info("Getting restricting nodes data from collection")
    if type(region_id)== int:
        cursor = database["ways"].find({"landuse": {"$in": landuse_types}, "region_id":  region_id}
                                    , {"nodes": 1, "_id": 0}, allow_disk_use=True)
    else:
        cursor = database["ways"].find({"landuse": {"$in": landuse_types}, "region_id": {"$in": region_id}}
                                   , {"nodes": 1, "_id": 0}, allow_disk_use=True)
    data = list(cursor)
    final_list = []
    for element in data:
        final_list.extend(element["nodes"])

    # log.info("Data retreived successfully")
    return final_list

def query_get_nodes_from_way(landuse: list, attributes: dict, col):
    log.info("Getting allowable nodes data from collection")
    cursor = col.find({"landuse": {"$in": landuse}}, attributes, allow_disk_use = True)
    data = list(cursor)
    final_list = []
    for element in data:
        final_list.extend(element["nodes"])

    #log.info("Data retreived successfully")
    return final_list

def iterate_nodes_list(nodes_allowable: list, database):
    landuse_types = ["residential", "nature_reserve", "construction", "military"]
    start = time.time()
    for node in nodes_allowable:
        region_ids = get_region_ids(node, database)
        nodes_restricted_base_region = get_restricting_nodes(region_ids[0], landuse_types, database)
        log.info(f"Looking for closest restriction for node: {node}")

        node_final_region = find_closest_restriction(node, database["nodes"], nodes_restricted_base_region)
        if node_final_region["is_buildeable"] == True:
            nodes_restricted_neighbour_region = get_restricting_nodes(region_ids[1:], landuse_types, database)
            node_final_neighbours = find_closest_restriction(node, database["nodes"], nodes_restricted_neighbour_region)
            insert_to_collection(node_final_neighbours, database["nodes_final"])
            time.sleep(1)
            end = time.time()
            log.info(f"Finding closes neighbour took {(end -start)/60} minutes")
        else:
            insert_to_collection(node_final_region, database["nodes_final"])
            time.sleep(1)
            end = time.time()
            log.info(f"Finding closes neighbour took {(end - start)/60} minutes")


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
        elif (closest_distance >= current_calculated_distance):
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

    #cursor_check = collection.find({"id": document["id"]})
    #if cursor_check.count() == 0:
    #    log.info("Cursor is empty")
    #    collection.insert_one(document)
    #else:
    #    log.info(f"The cursor for {document['id']} already exists")

    collection.insert_one(document)

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