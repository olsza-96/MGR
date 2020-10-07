import requests
import logging as log
import json
import pathlib as p
from typing import List, TypedDict

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

    with region_list_path.open("r") as read_file:
        for line in read_file:
            json_file_name: str = line.rstrip()
            json_file_path: p.Path = json_folder_path.joinpath(f"{json_file_name}.json")

            # Check if the JSON file already exists. If it doesn't, get the data and save it
            if not json_file_path.exists():
                log.info(f"Getting data for {json_file_name}")

                raw_data = get_map_data("http://overpass-api.de/api/interpreter", json_file_name)

                # Remove irrelevant keys from the data
                raw_data = {k: v for k, v in raw_data.items() if k == "elements"}

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

    response: requests.Response = requests.get(url, params={"data":  query_regions})

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
    """ Saves the data of a region nodes to a JSON file

    Args:
        data_nodes (List[Node]): The list of nodes containing the data
        file_name (str): The name of the file
        folder_path (Path): The path to the folder where files will be saved
    """

    file_path: p.Path = folder_path.joinpath(f"{file_name}.json")

    with file_path.open("w") as written_file:
        log.info(f"Saving file {file_name}.json")

        json.dump(data_nodes, written_file)


if __name__ == "__main__":
    get_data_for_each_region("list_regions.txt", "region_files")
