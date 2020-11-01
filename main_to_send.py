import requests
import logging as log
import json
import pathlib as p
from typing import List, TypedDict
import math
import glob
import errno

log.getLogger().setLevel(log.INFO)
log.basicConfig(format="%(asctime)s - [%(levelname)s]: %(message)s", datefmt="%H:%M:%S")


class Node(TypedDict):
    """Class definition for Node type hint

    Attributes:
        type (str):
        id (int):
        lat (float):
        lon (float):
        way_id (int):
        landuse (str):
    """

    type: str
    id: int
    lat: float
    lon: float
    way_id: int
    landuse: str


def get_data_for_each_region(file_name: str, folder_name: str) -> None:
    """Gets the data of regions in JSON format and saves them to a folder if
    they don't already exist

    Args:
        file_name (str): The name of the file containing the list of
        regions
        folder_name (str): The folder where the JSON files are saved
    """

    region_list_path: p.Path = p.Path.cwd().joinpath(file_name)
    json_folder_path: p.Path = p.Path.cwd().joinpath(folder_name)

    with region_list_path.open(mode="r", encoding="utf-8") as read_file:
        for line in read_file:
            json_file_name: str = line.rstrip()
            json_file_path: p.Path = json_folder_path.joinpath(f"{json_file_name}.json")

            # Check if the JSON file already exists. If it doesn't, get the data and save it
            if not json_file_path.exists():
                log.info(f"Getting data for {json_file_name}")

                raw_data = get_map_data("http://overpass-api.de/api/interpreter", json_file_name)

                save_file(raw_data, json_file_name,  p.Path.cwd().joinpath('raw_data'))

                # Remove irrelevant keys from the data
                raw_data = {k: v for k, v in raw_data.items() if k == "elements"}
                save_file(raw_data, json_file_name, p.Path.cwd().joinpath('removed_keys'))

                data_nodes: List[Node] = get_nodal_data(raw_data["elements"])

                save_file(data_nodes, json_file_name, json_folder_path)
            else:
                log.warning(f"File for {json_file_name} already exists")


def get_map_data(url: str, boundary_name: str):
    """Gets the required data from OpenStreetMap

    Args:
        url (str): The URL to the OpenStreetMap API endpoint
        boundary_name (str): The name of the region to get the data from

    Returns:
        ?:
    """

    query_regions: str = f"""
    [out:json];
    area["boundary"="administrative"][admin_level = 6][name = "{boundary_name}"];
    (
       way
       ["landuse"~"residential|nature_reserve|farmland|meadow|brownfield"]
       (area);
       node(w)
       (area);
    );
    (._;>;);
    out meta;
    """

    response: requests.Response = requests.get(url, params={"data": query_regions})

    return response.json()


def get_nodal_data(data):
    """Creates a list of dictionaries being one dictionary the info on one node

    Args:
        data (?):

    Returns:
        List[Node]: A list of nodes containing the region data
    """

    nodes = get_list_data(data, "node", ["type", "id", "lat", "lon"])
    ways = get_list_data(data, "way", ["id", "nodes", "tags"])

    return append_way_data(nodes, ways)


def get_list_data(data, key_name: str, attributes: List[str]):
    """Gets data from a dictionary based on chosen key type (node/way)

    Args:
        data (?):
        key_name (str):
        attributes List[str]:

    Returns:
        List[str]:
    """

    if key_name in ["node", "way"]:
        result = []

        for line in data:
            if line["type"] == key_name:
                result.append({k: v for k, v in line.items() if k in attributes})

        return result
    else:
        log.error(f"Key \"{key_name}\" not supported")


def append_way_data(nodes, ways) -> List[Node]:
    """Searches for information regarding nodal membership to ways and updates
    each node with way_id and landuse type

    Args:
        nodes (List[str]):
        ways (List[str]):

    Returns:
        List[Node]:
    """

    for line in nodes:
        current_id = line["id"]
        line.update(search_ways(ways, current_id))

    return nodes


def search_ways(ways, node_id: int):
    """Adds information on way_id and landuse type for each node in the node list

    Args:
        ways (?):
        node_id (int):

    Returns:
        ?:
    """

    result = {}

    for line in ways:
        if node_id in line["nodes"]:
            result["way_id"] = line["id"]
            result["landuse"] = line["tags"]["landuse"]

    return result


def save_file(data_nodes: List[Node], file_name: str, folder_path: p.Path) -> None:
    """ Saves the data of a region nodes to a new JSON file

    Args:
        data_nodes (List[Node]): The list of nodes containing the data
        file_name (str): The name of the file
        folder_path (Path): The path to the folder where files will be saved
    """

    file_path: p.Path = folder_path.joinpath(f"{file_name}.json")

    if not file_path.exists():
        file_path.touch()

        with file_path.open("w") as written_file:
            log.info(f"Saving file {file_name}.json")

            json.dump(data_nodes, written_file)
    else:
        log.warning(f"File for {file_path.stem} already exists")


def get_json_file(folder_name: str):
    """Reads JSON file created in the previous steps and add it to list of nodes

       Args:
           folder_name (str): The name of the folder where all the JSON files are stored:

       """

    json_folder_path: p.Path = p.Path.cwd().joinpath(folder_name+'/*.json')
    log.info(f"Current folder path {json_folder_path}")
    #i dont know why but it doesnt work with proper folder name
    files_to_read = glob.glob('/Users/Olga/PycharmProjects/MGR_Project/MGR/regions_files/*.json')
    data_nodes = []

    for file in files_to_read:
        log.info(f"Getting data for {file}")
        try:
            with open(file, mode="r", encoding="utf-8") as json_read_file:
                read_file = json.load(json_read_file)
                data_nodes.append(read_file)
                log.info(f"Current length of file is  {len(read_file)}")
                log.info(f"Current lenght of data is  {len(data_nodes)}")
                #wtf why length of read file is good, but after appending it to the overall list is 1 ?
                break

        except IOError as exc:
            if exc.errno != errno.EISDIR:
                raise


    return data_nodes

def add_usage_tag(data_nodes: List[Node]):
    """
    adds filter tag based on if there can be windfarm there or not
    0 - no possibility of windfarms
    1 - possibility of windfarms
    :param data_nodes:
    :return data_nodes:
    """

    restrictions = get_filtered_nodes(data_nodes, ['residential', 'nature_reserve'], 'landuse')

    for Node in data_nodes:
        if Node['landuse'] in ['residential', 'nature_reserve']:
            Node['filter_tag'] = 0
        else:
            #look for the nearest node that constraints the windfarm, meaning that the distance is below 1.56 km
            #if the closest restricting node is 1.56 or more, add tag equal to 1 and save the distance for further purposes
            Node['filter_tag'], Node['distance_restriction'] = look_for_nearest_node(Node, restrictions)

    return data_nodes

def get_filtered_nodes(data_nodes, list_filter_nodes, dict_key):
    """
    filters nodes according to chosen filter
    Arguments:
        data_nodes (List)
        list_filter_nodes (List)
        dict_key (str)
    """

    filtered_list = []
    for Node in data_nodes:
        if Node[dict_key] in list_filter_nodes:
            filtered_list.append(Node)

    return filtered_list

def look_for_nearest_node(goal_node, restrictions):
    """looks for nearest restriction and distance between the current node and the restriction"""


    min_allowable_distance = 1.56  # the 10 times multiplicity of the average windfarm height in kilometers

    for res_node in restrictions:
        current_distance = calculate_distance(goal_node, res_node)
        if current_distance < min_allowable_distance:
            result = 0
            break
        else:
            result = 1

    return result, current_distance

def calculate_distance(node1, node2):
    """
    Calculates Haversine distance between two nodes, returns distance in kilometers between two nodes
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

    #log.info("The resultant distance between nodes is " + str(distance) + " km")

    return distance


if __name__ == "__main__":
    get_data_for_each_region("list_regions.txt", "regions_files")
    #data_nodes = get_json_file("regions_files")
    #print(data_nodes[0])
    #data_nodes = add_usage_tag(data_nodes)

    #allowable_nodes = get_filtered_nodes(data_nodes, [1], 'filter_tag')
    #print(allowable_nodes)