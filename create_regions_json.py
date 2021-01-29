import pathlib as p
import logging as log
from pymongo import MongoClient
from pymongo.errors import OperationFailure
import json


log.getLogger().setLevel(log.INFO)
log.basicConfig(format="%(asctime)s - [%(levelname)s]: %(message)s", datefmt="%H:%M:%S")

def iterate_region_list(region_fname) -> None:
    region_list_path: p.Path = p.Path.cwd().joinpath(region_fname)

    with region_list_path.open(mode="r", encoding="utf-8") as read_file:
        count = 1
        for line in read_file:
            region_name: str = line.rstrip()
            region_dict = create_json(region_name, count)
            insert_to_db("localhost", 27017, "regions", region_dict)


def create_json(region_name: str, count: int):

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

    return [{"id": count, "name": region_name, "ways": way_ids}]

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

if __name__ == "__main__":
    iterate_region_list("list_regions.txt")