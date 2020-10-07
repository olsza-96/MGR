#!/usr/bin/python3
# author: Jan Christoph Uhde <Jan@UhdeJC.com>
# requires lxml json and pyarango

import os
import sys
from lxml import etree
import json
from pyArango import connection

class RestaurantExtractor:
    """
    Class that allows event style reading of large xml files in osm
    format. It extracts nodes that represent a regular or fast_food restaurants.
    The extractor will call a given callback for each matching node.
    """
    def __init__(self, callback):
        self.node = None
        self.callback = callback

    def start(self, tag, attrib):
        #print("start %s %r" % (tag, dict(attrib)))
        if tag == "node":
            self.node = {}
            # conversion to float (c double) is important for
            # the geoindex to work
            self.node["lat"] = float(attrib["lat"])
            self.node["lon"] = float(attrib["lon"])

        if tag == "tag" and self.node:
            #filter roads
            if attrib["k"] == "highway":
                self.node = None
            #filter nont restaurant amenities
            if attrib["k"] == "amenity":
                if attrib["v"] == "restaurant" or attrib["v"] == "fast_food":
                    self.node["amenity"] = attrib["v"]
                else:
                    self.node = False

            #set name
            if attrib["k"] == "name":
                self.node["name"]=attrib["v"]

            if attrib["k"] == "cuisine":
                self.node["cuisine"]=attrib["v"]

    def end(self, tag):
        if tag == "node":
            if self.node and "amenity" in self.node:
                self.callback(self.node)
            self.node = None

    def close(self):
        self.__init__(self.callback)

def get_collection():
    """
    This function returns an ArangoDB collection.
    The function creates a connection to an ArangoDB instance,
    selects the _system database and returns a handle to the
    'places to eat' collection.
    """
    conn = connection.Connection(username="root", password="")
    db = conn["_system"]

    col_name="places_to_eat"
    if not db.hasCollection(col_name):
        return db, db.createCollection('Collection', name=col_name), col_name
    else:
        return db, db.collections[col_name], col_name

count = 0
def save_to_db(node, collection):
    """
    callback that is used to save every found location into the database
    """
    # skip a location if it does not provide a name
    if not "name" in node:
        return

    collection.createDocument(node).save()

    # show some progress during import
    global count
    count += 1
    print(count)

def process_osm(file_handle, collection):
    """
    Create and execute parser that extracts.
    The parser will store the found locations into collection.
    """
    # we use a lambda to bind the collection to the function so we
    # satisfy the ctor of the extractor which takes only one argument
    parser = etree.XMLParser(target = RestaurantExtractor(lambda node : save_to_db(node,collection)))
    etree.parse(file_handle, parser)
    #create geo index
    collection.ensureGeoIndex(["lat","lon"])
    return 0

def print_usage():
    prog_name=sys.argv[0]
    usage="""
Usage Error - Please invoke the command as follows:
    {name} import - imports the data
    {name} query <lat> <lon> | lat,lon as double - executes a query
""".format(name=prog_name)
    print(usage)
    return 1

def query1(db, col, col_name, lat, lon):
    q="""FOR d IN {col_name}
            FILTER distance(d.lat, d.lon, {lat}, {lon}) < 1000
            LIMIT 5
            RETURN d
""".format(col_name=col_name, lat=lat, lon=lon)
    print("running query: " + q)
    result = db.AQLQuery(q, bindVars={}, rawResults=True)
    for i in result:
        print(i['name'])

def evaluate_args():
    alen =  len(sys.argv)
    print(sys.argv)

    if alen == 3 and sys.argv[1] == "import":
        _, col, _ = get_collection()
        with open(sys.argv[2]) as fh:
            return process_osm(fh,col)

    if alen == 4 and sys.argv[1] == "query1":
        db, col, col_name = get_collection()
        lat = float(sys.argv[2])
        lon = float(sys.argv[3])
        return query1(db, col, col_name, lat,lon)

    return print_usage()

if __name__ == "__main__":
    sys.exit(evaluate_args())