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


def get_data_for_each_region(file_name: str, folder_name: str) -> None:
    """Gets the data of regions in JSON format and saves them to a folder if
    they don't already exist

    Args:
        file_name (str): The name of the file containing the list of
        regions
        folder_name (str): The folder where the JSON files will be saved
    """

    region_list_path: p.Path = p.Path.cwd().joinpath(file_name)
    json_folder_path: p.Path = p.Path.cwd().joinpath(folder_name)

    raw_nodes_landuse: str = "residential|nature_reserve|farmland|meadow|brownfield|construction|orchard|grass|military"

    if not json_folder_path.exists():
        json_folder_path.mkdir()


    with region_list_path.open(mode="r", encoding="utf-8") as read_file:
        count = 1
        for line in read_file:
            json_file_name: str = line.rstrip()
            json_file_path_nodes: p.Path = json_folder_path.joinpath(f"{json_file_name}_nodes.json")
            json_file_path_ways: p.Path = json_folder_path.joinpath(f"{json_file_name}_nodes.json")
            # Check if the JSON file already exists. If it doesn't, get the data and save it
            if not json_file_path_nodes.exists() and not json_file_path_ways.exists():
                log.info(f"Getting data for {json_file_name}")

                raw_region_data = get_raw_region_data("http://overpass-api.de/api/interpreter", json_file_name,
                                                      raw_nodes_landuse, 1)

                log.info(f"Data for {json_file_name} downloaded with success")
                region_data = get_region_data(raw_region_data, count)
                # save data for available nodes in one file and restricting nodes in another
                save_region_file(region_data, json_file_name, json_folder_path)
                count = count + 1
            else:
                log.warning(f"File for {json_file_name} already exists")
                count = count + 1

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
        total=5,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    response: requests.Response = requests.get(url, params={"data": query_regions})
    log.info(f"The response from the server: {response.status_code}")
    return response.json()



def get_region_data(raw_region_data: Any, count: int) -> Region:
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
            node["region_id"] = count
            raw_nodes.append(node)
        elif element["type"] == "way":
            way: Way = {k: v for k, v in element.items() if k in way_relevant_keys}
            way["landuse"] = way.pop("tags").pop("landuse")
            way["region_id"] = count
            ways.append(way)

    return {"nodes": raw_nodes, "ways": ways}


def save_region_file(region_data: Region, file_name: str, folder_path: p.Path) -> None:
    """Saves the data of a region to a new JSON file

    Args:
        region_data (Region): A Region object containing the data
        file_name (str): The name of the file
        folder_path (Path): The path to the folder where the file will be saved
    """

    file_path_node: p.Path = folder_path.joinpath(f"{file_name}_nodes.json")
    file_path_way: p.Path = folder_path.joinpath(f"{file_name}_ways.json")
    if not file_path_node.exists():
        file_path_node.touch()

        with file_path_node.open(mode="w", encoding="utf-8") as written_file:
            log.info(f"Saving file {file_name}_nodes.json")

            json.dump(region_data["nodes"], written_file)
    else:
        log.warning(f"File for {file_path_way} already exists")

    if not file_path_way.exists():
        file_path_way.touch()

        with file_path_way.open(mode="w", encoding="utf-8") as written_file:
            log.info(f"Saving file {file_name}_ways.json")

            json.dump(region_data["ways"], written_file)
    else:
        log.warning(f"File for {file_path_way} already exists")

if __name__ == "__main__":
    get_data_for_each_region("list_regions.txt", "region_data")
