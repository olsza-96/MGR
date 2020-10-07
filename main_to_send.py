import requests
import os
import json


def get_data_each_region():
    ####### get the list of regions for which to download the data from OSM

    with open('list_regions.txt', 'r') as file:
        print(file)
        for item in file:
            item = item.rstrip("\n")
            #start by checking if file already exists:
            exists = os.path.isfile('regions_files/{}.json'.format(item))
            if exists:
                print("File for " + str(item) + " already exists")
                print("Getting data for next region")
                pass
            else:
                print("Getting data for " + str(item))
                input_data = get_map_data("http://overpass-api.de/api/interpreter", item)
                #remove irrelevant keys from the data
                input_data = {k: v for k, v in input_data.items() if k == 'elements'}


                #print("Input data is " +str(input_data))
                #save_file(input_data, item)

                nodes = get_nodal_data(input_data['elements'])

                save_file(nodes, item)
                #at this point I get a list of dictionaries with information on each node. the keys for each node are: 'type', 'id', 'lat', 'lon', 'way_id', 'landuse', where type is always 'node'


def get_map_data(overpass_add, boundary_name):
    # gets required data from OPENSTREETMAP


    query_not_used = """
    [out:json];
    (
       way
       ["landuse"~"residential|nature_reserve|farmland|meadow|brownfield"]
       (52.2592, 20.5198, 52.2832, 20.5764);
       node(w)
       (52.26209, 20.7632, 52.26668, 20.77592);

    );
    (._;>;); 
    out meta;
    """

    query_regions = """
    [out:json];
    area["boundary"="administrative"][admin_level = 6][name = "{}"];
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

    response = requests.get(overpass_add, params={'data':  query_regions.format(boundary_name)})

    return response.json()

def get_nodal_data(inputs):
    #create list of dictionaries, one dictionary is info on one node

    nodes = get_list_data(inputs, 'node', ['type', 'id', 'lat', 'lon'])
    ways = get_list_data(inputs, 'way', ['id', 'nodes', 'tags'])

    #print("Resultant nodes " + str(ways))

    return append_way_data(nodes, ways)

def get_list_data(input_ls, key_name, list_params):
    #returns data from JSON file based on chosen type (node/way)
    result = []

    for line in input_ls:
        if line['type'] == key_name:
            result.append({k: v for k, v in line.items() if k in list_params})

    return result

def append_way_data(nodes, ways):
    #searches for information regarding nodal membership to ways, updates each node with way id and landuse type

    for line in nodes:
        current_id = line['id']
        line.update(search_ways(ways, current_id))
        #print("After update: " +str(line))

    return nodes

def search_ways(ways, node_id):
    #add information on way id and landuse type for each node in the nodes list

    result = {}
    for line in ways:
        if node_id in line['nodes']:
            result['way_id'] = line['id']
            result['landuse'] = line['tags']['landuse']

    return result

def save_file(data_file, filename):

    """for key in data_file.keys():
        with open('powiaty_files/{}.json'.format(filename+"_"+key), 'w') as f:
            json.dump(data_file[key], f)"""
    print("Name file "  + str(filename))
    with open('regions_files/{}.json'.format(filename), 'w') as f:
        json.dump(data_file, f)


if __name__ == "__main__":
    get_data_each_region()
