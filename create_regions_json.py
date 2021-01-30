import pathlib as p
import logging as log
from pymongo import MongoClient
from pymongo.errors import OperationFailure
import json


log.getLogger().setLevel(log.INFO)
log.basicConfig(format="%(asctime)s - [%(levelname)s]: %(message)s", datefmt="%H:%M:%S")

def iterate_region_list(region_fname: str, neighbour_dict: dict, all_region_numbers: dict) -> None:
    region_list_path: p.Path = p.Path.cwd().joinpath(region_fname)

    all_regions = []
    with region_list_path.open(mode="r", encoding="utf-8") as read_file:
        for line in read_file:
            region_name: str = line.rstrip()
            neighbour_numbers = get_neighbour_number_list(region_name, neighbour_dict, all_region_numbers)
            region_list = create_json(region_name, region_numbers[region_name], neighbour_numbers)
            all_regions.extend(region_list)
            insert_to_db("localhost", 27017, "regions", region_list)
    log.info(f"Regiony {all_regions}")
    save_file("all_regions_file", all_regions)

def get_neighbour_number_list(region_name: str, neighbour_dict: dict, region_numbers: dict):
    list_numbers = []
    for element in neighbour_dict[region_name]:
        list_numbers.append(region_numbers[element])

    return list_numbers
def create_json(region_name: str, count: int, neighbour_nums: list):

    """{
        "id": 1,
        "name": "Bytom",
        "ways": [
            379729636,
            379729637,
            379729638
        ]
    }"""
    way_ids = get_ways_id('localhost', 27017, count, 'ways')

    return [{"id": count, "name": region_name, "ways": way_ids, "neighbours": neighbour_nums}]

def get_ways_id(host: str, port: int, region_id: int, collection: str):

    connection = MongoClient(host, port)
    try:
        connection.server_info()
        log.info(f"Connected successfully")
    except OperationFailure:
        log.error(f"Could not connect to db")

    with connection:
        db = connection.Poland_spatial_data
        current_collection = db[collection]

        query = {"region_id": region_id}
        attributes = {"id": 1, "_id": 0}
        cursor = current_collection.find(query, attributes)
        way_ids = []
        for doc in cursor:
            way_ids.append(doc["id"])

        return way_ids

def save_file(fname: str, file: list) -> None:

    file_path: p.Path =  p.Path.cwd().joinpath(fname+".json")
    with file_path.open(mode="w", encoding="utf-8") as written_file:
        log.info(f"Saving file {fname}.json")
        json.dump(file, written_file)
    log.info(f"Data for {fname} saved correctly")

def insert_to_db(host: str, port: int, collection: str, doc: dict) -> None:

    connection = MongoClient(host, port)
    try:
        connection.server_info()
        log.info(f"Connected successfully")
    except OperationFailure:
        log.error(f"Could not connect to db")

    with connection:
        db = connection.Poland_spatial_data
        current_collection = db[collection]
        current_collection.insert(doc)

def get_neighbour_list(file_name: str):

    file_path: p.Path = p.Path.cwd().joinpath(file_name)
    neighbour_dict = {}
    with file_path.open(mode="r", encoding="utf-8") as read_file:
        for line in read_file:
            name = line.rstrip().split(':')[0].rstrip()
            neighbours = line.rstrip().split(':')[1].split(";")
            neighbours = list(filter(lambda x: x!='', neighbours))
            neighbours = [x.lstrip() for x in neighbours]
            neighbour_dict[name] = neighbours
    return neighbour_dict

def create_region_number_pairs(file_name):
    file_path: p.Path = p.Path.cwd().joinpath(file_name)
    region_count = {}
    with file_path.open(mode="r", encoding="utf-8") as read_file:
        count = 1
        for line in read_file:
            region = line.rstrip()
            region_count[region] = count
            count = count + 1

    return region_count

if __name__ == "__main__":
    region_numbers = create_region_number_pairs("list_regions.txt")
    neighbours = get_neighbour_list("list_neigbour_regions.txt")
    iterate_region_list("list_regions.txt", neighbours, region_numbers)