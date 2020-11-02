import requests
import logging as log
import json
import pathlib as p
from typing import Any, List, TypedDict

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

    if not json_folder_path.exists():
        json_folder_path.mkdir()

    with region_list_path.open(mode="r", encoding="utf-8") as read_file:
        for line in read_file:
            json_file_name: str = line.rstrip()
            json_file_path: p.Path = json_folder_path.joinpath(f"{json_file_name}.json")

            # Check if the JSON file already exists. If it doesn't, get the data and save it
            if not json_file_path.exists():
                log.info(f"Getting data for {json_file_name}")

                raw_region_data = get_raw_region_data("http://overpass-api.de/api/interpreter", json_file_name)
                region_data = get_region_data(raw_region_data)

                save_region_file(region_data, json_file_name, json_folder_path)
            else:
                log.warning(f"File for {json_file_name} already exists")


def get_raw_region_data(url: str, boundary_name: str) -> Any:
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


def get_region_data(raw_region_data: Any) -> Region:
    """

    """

    # Tuples to define the keys that are relevant to us
    node_keys = ("id", "lat", "lon")
    way_keys = ("id", "nodes", "tags")

    node_list: List[RawNode] = list()
    way_list: List[Way] = list()

    for element in [element for element in raw_region_data["elements"]]:
        if element["type"] == "node":
            node: RawNode = {k: v for k, v in element.items() if k in node_keys}
            node_list.append(node)
        elif element["type"] == "way":
            way: Way = {k: v for k, v in element.items() if k in way_keys}
            way["landuse"] = way.pop("tags").pop("landuse")
            way_list.append(way)

    return {"nodes": get_region_nodes(node_list, way_list)}


def get_region_nodes(node_list, way_list) -> List[RegionNode]:
    """

    """

    for node in node_list:
        for way in way_list:
            if node["id"] in way["nodes"]:
                node["way_id"] = way["id"]
                node["landuse"] = way["landuse"]

    return node_list


def save_region_file(region_data: Region, file_name: str, folder_path: p.Path) -> None:
    """Saves the data of a region nodes to a new JSON file

    Args:
        region_data (Region): A Region object containing the data
        file_name (str): The name of the file
        folder_path (Path): The path to the folder where the file will be saved
    """

    file_path: p.Path = folder_path.joinpath(f"{file_name}.json")

    if not file_path.exists():
        file_path.touch()

        with file_path.open("w") as written_file:
            log.info(f"Saving file {file_name}.json")

            json.dump(region_data, written_file)
    else:
        log.warning(f"File for {file_path.stem} already exists")


if __name__ == "__main__":
    get_data_for_each_region("list_regions.txt", "region_data_files")
