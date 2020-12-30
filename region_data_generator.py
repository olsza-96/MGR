import math
import requests
import logging as log
import json
import pathlib as p
import time
import random
from typing import Any, List, TypedDict, Tuple
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


log.getLogger().setLevel(log.INFO)
log.basicConfig(format="%(asctime)s - [%(levelname)s]: %(message)s", datefmt="%H:%M:%S")


class RawNode(TypedDict):
    """Class definition for RawNode type hint

    Attributes:
        id (int):
        lat (float):
        lon (float):
    """

    id: int
    lat: float
    lon: float


class Way(TypedDict):
    """Class definition for Way type hint

    Attributes:
        id (int):
        nodes (List[int]):
        landuse (str):
    """

    id: int
    nodes: List[int]
    landuse: str


class RegionNode(TypedDict):
    """Class definition for RegionNode type hint

    Attributes:
        id (int):
        lat (float):
        lon (float):
        way_id (int):
        landuse (str):
    """

    id: int
    lat: float
    lon: float
    way_id: int
    landuse: str


class Region(TypedDict):
    """Class definition for Region type hint

    Attributes:
        nodes (List[RegionNode]):
    """
    nodes: List[RegionNode]


def get_data_for_each_region(file_name: str, folder_name: str, folder_name_restrictions: str) -> None:
    """Gets the data of regions in JSON format and saves them to a folder if
    they don't already exist

    Args:
        file_name (str): The name of the file containing the list of
        regions
        folder_name (str): The folder where the JSON files will be saved
    """

    region_list_path: p.Path = p.Path.cwd().joinpath(file_name)
    json_folder_path: p.Path = p.Path.cwd().joinpath(folder_name)
    json_folder_path_restrictions: p.Path = p.Path.cwd().joinpath(folder_name_restrictions)

    raw_nodes_landuse: str = "residential|nature_reserve|farmland|meadow|brownfield|construction|orchard|grass|military"
    raw_neighbours_restriction_landuse: str = "residential|nature_reserve|construction|military"

    if not json_folder_path.exists():
        json_folder_path.mkdir()
    if not json_folder_path_restrictions.exists():
        json_folder_path_restrictions.mkdir()

    with region_list_path.open(mode="r", encoding="utf-8") as read_file:
        for line in read_file:
            json_file_name: str = line.rstrip()
            json_file_path: p.Path = json_folder_path.joinpath(f"{json_file_name}.json")
            json_file_name_restrictions: str = json_file_name + "_restricting"

            # Check if the JSON file already exists. If it doesn't, get the data and save it
            if not json_file_path.exists():
                log.info(f"Getting data for {json_file_name}")

                neighbour_regions = get_neighbour_data('list_neigbour_regions.txt', json_file_name)
                raw_region_data = get_raw_region_data("http://overpass-api.de/api/interpreter", json_file_name,
                                                      raw_nodes_landuse, 1)

                log.info(f"Getting data for {json_file_name} neighbours")
                for neighbour in neighbour_regions:
                    log.info(f"Getting data for {neighbour}")
                    raw_neighbours_data = get_raw_region_data("http://overpass-api.de/api/interpreter", neighbour,
                                                              raw_neighbours_restriction_landuse, 1)
                    #log.info(raw_neighbours_data)
                    raw_region_data['elements'] = raw_region_data['elements'] + raw_neighbours_data['elements']
                log.info(f"Data for {json_file_name} and its neighbour regions downloaded with success")
                region_data = get_region_data(raw_region_data)
                # save data for available nodes in one file and restricting nodes in another
                save_region_file(region_data["available nodes"], json_file_name, json_folder_path)
                save_region_file(region_data["restricting nodes"], json_file_name_restrictions,
                                json_folder_path_restrictions)
            else:
                log.warning(f"File for {json_file_name} already exists")


def get_neighbour_data(file_name: str, region: str):
    """Gets the information on neighbouring regions for each region"""

    region_neighbours_path: p.Path = p.Path.cwd().joinpath(file_name)

    with region_neighbours_path.open(mode="r", encoding="utf-8") as read_file:
        for line in read_file:
            if region + " - " in line:
                neighbour_raw_line: str = line.rstrip(" \n").replace(f"{region} - ", "").rsplit("; ")
                neighbours_list = list(filter(None, neighbour_raw_line))
                neighbours_list = [x.rstrip(";") for x in neighbours_list]

    return neighbours_list


def get_raw_region_data(url: str, boundary_name: str, landuse_types: str, try_number: int) -> Any:
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
       ["landuse"~"{landuse_types}"]
       (area);
       node(w)
       (area);
    );
    (._;>;);
    out meta;
    """

    retry_strategy = Retry(
        total=10,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    try:
        response: requests.Response = requests.get(url, params={"data": query_regions})
        log.info(f"The response from the server: {response.status_code}")
        return response.json()

    except json.decoder.JSONDecodeError:
        log.info(f"Trying to reconnect in {2 ** try_number + random.random() * 0.01} seconds")
        time.sleep(2**try_number + random.random() * 0.01)
        get_raw_region_data(url, boundary_name, landuse_types, try_number + 1)


def get_region_data(raw_region_data: Any) -> Region:
    """
    Extracts only relevant data from raw OSM data
    """

    # Tuples to define the keys that are relevant to us
    node_relevant_keys: Tuple[str, ...] = ("id", "lat", "lon")
    way_relevant_keys: Tuple[str, ...] = ("id", "nodes", "tags")

    raw_nodes: List[RawNode] = list()
    ways: List[Way] = list()

    for element in [element for element in raw_region_data["elements"]]:
        if element["type"] == "node":
            node: RawNode = {k: v for k, v in element.items() if k in node_relevant_keys}
            raw_nodes.append(node)
        elif element["type"] == "way":
            way: Way = {k: v for k, v in element.items() if k in way_relevant_keys}
            way["landuse"] = way.pop("tags").pop("landuse")
            ways.append(way)

    available_nodes, restricting_nodes = get_region_nodes(raw_nodes, ways)
    return {"available nodes": available_nodes, "restricting nodes": restricting_nodes}


def get_region_nodes(raw_nodes: List[RawNode], ways: List[Way]) -> List[RegionNode]:
    """
    Looks for nodes where building wind farm is possibile
    """
    # Tuple to define the lands that are not buildable
    no_buildable_lands: Tuple[str, ...] = ("residential", "nature_reserve")

    restricting_nodes: List[RegionNode] = list()

    for node in raw_nodes:
        for way in ways:
            if node["id"] in way["nodes"]:
                node["way_id"] = way["id"]
                node["landuse"] = way["landuse"]

                if way["landuse"] in no_buildable_lands:
                    restricting_nodes.append(node)

    return add_closest_distance_restriction([node for node in raw_nodes if node["landuse"] not in no_buildable_lands],
                                            restricting_nodes), restricting_nodes


def add_closest_distance_restriction(region_nodes: List[RegionNode], restricting_nodes: List[RawNode]):
    """
    Calculates distances between nodes in sets of valid and restricting nodes.
    Returns node parameters that is valid for building the wind farm.
    """
    min_allowable_distance: float = 0.5  # currently 500 m

    for node in region_nodes:
        closest_distance: float = 0

        for restricting_node in restricting_nodes:
            current_calculated_distance = calculate_distance(node, restricting_node)

            if closest_distance == 0:
                closest_distance = current_calculated_distance

            if current_calculated_distance < min_allowable_distance:
                closest_distance = 0
                break
            elif (closest_distance > current_calculated_distance):
                closest_distance = current_calculated_distance

        node["closest_distance_restriction"] = closest_distance

    return [node for node in region_nodes if node["closest_distance_restriction"] != 0]


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


def save_region_file(region_data: Region, file_name: str, folder_path: p.Path) -> None:
    """Saves the data of a region to a new JSON file

    Args:
        region_data (Region): A Region object containing the data
        file_name (str): The name of the file
        folder_path (Path): The path to the folder where the file will be saved
    """

    file_path: p.Path = folder_path.joinpath(f"{file_name}.json")

    if not file_path.exists():
        file_path.touch()

        with file_path.open(mode="w", encoding="utf-8") as written_file:
            log.info(f"Saving file {file_name}.json")

            json.dump(region_data, written_file)
    else:
        log.warning(f"File for {file_path.stem} already exists")


if __name__ == "__main__":
    get_data_for_each_region("list_regions.txt", "region_data_05km_available", "region_data_05km_restrictions")
