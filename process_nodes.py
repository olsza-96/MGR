from pymongo import MongoClient
from pymongo.errors import OperationFailure
import math
import logging as log
import time
import ssl

log.getLogger().setLevel(log.INFO)
log.basicConfig(format="%(asctime)s - [%(levelname)s]: %(message)s", datefmt="%H:%M:%S")

def get_nodes_from_way(region_id: int):
    log.info(f"Connecting to the database")

    connection = MongoClient("mongodb+srv://olga:MGR12345%21@sandbox.iseuv.mongodb.net/Poland_spatial_data?retryWrites=true&w=majority", authSource = "admin",  ssl_cert_reqs=ssl.CERT_NONE)

    try:
        connection.server_info()
        log.info(f"Connected successfully")
    except OperationFailure:
        log.error(f"Could not connect to db")

    db = connection.Poland_spatial_data
    current_collection = db["testing_col"]
    allowable_landuse = ["farmland", "meadow", "brownfield", "orchard", "grass"]

    allowable_nodes = query_get_nodes_from_way(allowable_landuse, current_collection, region_id)
    restricting_landuse = ["residential", "nature_reserve", "construction", "military"]
    restricting_nodes = query_get_nodes_from_way(restricting_landuse,  current_collection, region_id)
    region_neighbours = get_region_ids(region_id, db["regions"])
    iterate_nodes_list(allowable_nodes, restricting_nodes, current_collection, region_neighbours)



def get_region_ids(region_id: int , collection):
    cursor = collection.find({"id": region_id}, {"_id": 0, "neighbours": 1})

    result = list(cursor)
    if len(result)!=0:
        return result[0]["neighbours"]
    else:
        return 0


def get_restricting_nodes(region_id, landuse_types:list, collection):
    log.info("Getting restricting nodes data from collection")
    if type(region_id)== int:
        cursor = collection.find({"landuse": {"$in": landuse_types}, "region_id":  region_id}
                                 , {"id": 1, "coordinates": 1, "_id": 0}, allow_disk_use=True)
    else:
        cursor = collection.find({"landuse": {"$in": landuse_types}, "region_id": {"$in": region_id}}
                                 , {"id": 1, "coordinates": 1, "_id": 0}, allow_disk_use=True)
    data = list(cursor)

    return data

def query_get_nodes_from_way(landuse: list,  col, region_id: int):
    log.info(f"Getting allowable nodes data from collection for region {region_id}")
    cursor = col.find({"landuse": {"$in": landuse}, "region_id": region_id}, {"id": 1, "coordinates": 1, "_id": 0}, allow_disk_use = True)
    #returns list of nodes where i can build
    data = list(cursor)

    return data

def iterate_nodes_list(nodes_allowable: list, restricting_nodes: list, collection, region_neighbours:list):


    landuse_types = ["residential", "nature_reserve", "construction", "military"]
    nodes_restricted_neighbour_region = get_restricting_nodes(region_neighbours, landuse_types, collection)
    for node in nodes_allowable:
        start = time.time()
        log.info(f"Looking for restrictions for node: {node['id']}")
        #check if node has already the attributes
        cur = collection.find_one({"id": node["id"], "is_buildeable": {"$exists": True}})
        if cur!=None:
            log.info(f"Node already calculated")
            continue
        else:
            node_final_region = find_closest_restriction(node, restricting_nodes, 'curr_region')
            if node_final_region["is_buildeable"] == True:
                node_final_neighbours = find_closest_restriction(node_final_region, nodes_restricted_neighbour_region, 'neighbour_regions')
                insert_to_collection(node_final_neighbours, collection)
                time.sleep(1)
                end = time.time()
                log.info(f"Finding closes neighbour took {(end -start)/60} minutes")
            else:
                insert_to_collection(node_final_region, collection)
                time.sleep(1)
                end = time.time()
                log.info(f"Finding closes neighbour took {(end - start)/60} minutes")

def find_closest_restriction(node: dict, restricting_nodes: list, mode: str):

    min_allowable_distance: float = 0.5
    closest_distance: float =0

    final_res_node_id: int = 0
    #choose subset of restricting nodes where coordinates +- 3 km from the current node
    restricting_nodes = get_subset_restricting_nodes(node, restricting_nodes)
    if len(restricting_nodes) == 0:   #no restricting nodes in distance +- 4 km each coordinate
        if mode == "curr_region":
            node["is_buildeable"] = True
            node["closest_distance_restriction"] = 4.
            node["restricting_node_id"] = 999999999999
        else:
            pass

    else:
        for res_node in restricting_nodes:
            current_calculated_distance = calculate_distance(node["coordinates"], res_node["coordinates"])
            if closest_distance == 0:
                closest_distance = current_calculated_distance
                final_res_node_id = res_node["id"]
            if current_calculated_distance < min_allowable_distance:
                closest_distance = 0
                break
            elif (closest_distance >= current_calculated_distance):
                closest_distance = current_calculated_distance
                final_res_node_id = res_node["id"]
        node["closest_distance_restriction"] = closest_distance
        node["restricting_node_id"] = final_res_node_id
        if closest_distance >= min_allowable_distance:
            node["is_buildeable"] = True
        else:
            node["is_buildeable"] = False


    return node

def get_subset_restricting_nodes(current_node: dict, restricting_nodes:list):

    limit_coordinates = 0.04 #in coordinate system this would be around 3.33 km

    limit_lon = [current_node["coordinates"][0]- limit_coordinates, current_node["coordinates"][0]+limit_coordinates]
    limit_lat = [current_node["coordinates"][1]- limit_coordinates, current_node["coordinates"][1]+limit_coordinates]

    restricting_filtered = list(filter(lambda  d: d["coordinates"][0]>= limit_lon[0] and
                                                  d["coordinates"][0]<= limit_lon[1] and d["coordinates"][1]>=limit_lat[0]
                                                  and d["coordinates"][1]<=limit_lat[1], restricting_nodes))

    return restricting_filtered


def insert_to_collection(document: dict, collection):


    collection.update_one({"id": document["id"]}, {"$set": {"is_buildeable": document["is_buildeable"],
                                                            "restricting_node_id": document["restricting_node_id"],
                                                            "closest_distance_restriction": document["closest_distance_restriction"]}}
                          , upsert= False)

def calculate_distance(node1, node2):
    """Calculates Haversine distance in kilometers between two nodes
    :param node1, node2:
    :return distance:
    """
    lat1, lon1 = node1[1], node1[0]
    lat2, lon2 = node2[1], node2[0]
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
    #for i in range(45,47):
    get_nodes_from_way(1)