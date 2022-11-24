import requests
import json
from pprint import pprint
from pymongo import MongoClient, DESCENDING
import dateutil.parser
import re

client = MongoClient(
    "mongodb+srv://admin:admin@cluster0.8mptla5.mongodb.net/test")
db = client.vls  
collection_vlilles = db.vlilles 
collection_stations = db.stations  

# find a station from a partial name


def find_station(name_partial: str):
    requete = {"name": re.compile(
        name_partial, re.IGNORECASE), "city": re.compile("Lille", re.IGNORECASE)}
    cursor = collection_stations.find(requete)
    print("{} stations match".format(
        collection_stations.count_documents(requete)))
    l: list = []
    for i in cursor:
        l.append(i)
    return l



def input_range(min: int = 1, max: int = 5) -> int:
    while True:
        try:
            choix = input("choice [{},{}]:".format(min, max))
            choix = int(choix)
            if min <= choix <= max:
                return choix
        except:
            pass



def input_list(liste: list) -> int:
    if len(liste) == 0:
        return -1
    if len(liste) == 1:
        input()
        return 0
    return input_range(0, len(liste)-1)


print("""
Faites un choix:
   1. trouver une station avec un nom (avec quelques lettres)
    2. mettre à jour une station
    3. supprimer une station et ses données
    4. désactiver toutes les stations d'une zone donnée
    5. donner toutes les stations dont le ratio vélo/support total est inférieur à 20% entre 18h et 19h00 (du lundi au vendredi).
""")


choix: int = input_range()


if 1 <= choix <= 3:
    station_name = input("station name :")
    list_stations = find_station(station_name)
    for station in list_stations:
        pprint(station)
    if 2 <= choix <= 3:
        print("\n\nchoose a station")
        [print("    " + str(indice) + " : " + value["name"])
         for indice, value in enumerate(list_stations)]
        station_to_edit: int = input_list(list_stations)
        if station_to_edit == -1:
            exit("no station found")
        station_to_edit: dict = list_stations[station_to_edit]
        
        if choix == 2 :
            print("\n\nGo for editing station {}".format(station_to_edit))
            pprint(station_to_edit)
            
            [print("    " + str(key) + " : " + value + " : " + str(station_to_edit[value]))
            for key, value in enumerate(list(station_to_edit.keys())[1:])]
            print("\nfield to edit :")
            field_to_edit: int = input_list(list(station_to_edit.keys())[
                                            1:])  
            field_to_edit: str = list(station_to_edit.keys())[1:][field_to_edit]
            print("\n\nGo for editing field {}".format(field_to_edit))
           
            value = input("new value of the field :")
            
            collection_stations.update_one({"_id": station_to_edit["_id"]}, {
                                        "$set": {field_to_edit: value}})

        if choix == 3 :
            
            collection_vlilles.delete_many({"name": station_to_edit["name"]})

            
            collection_stations.delete_one(station_to_edit)

elif choix == 4:
   
    print("Draw your polygon geojson.io")
    geojson_file = input("geojson file :")
    geojson_file = json.loads(open(geojson_file).read().replace("\n",""))
    geoquery = { 
        "geo": {
            "$geoWithin": {
                "$geometry": geojson_file["features"][0]['geometry']
         }
    }}
    
    cursor = collection_stations.find(geoquery)
    for station in cursor:
        pprint(station)
    
    while True:
        what_to_do = input("disable / enable (d/e):")
        if what_to_do == "e" or what_to_do == "d":
            break

    if what_to_do == "e":
        cursor = collection_stations.update_many(geoquery, {"$set": {"status": True}})
    elif what_to_do == "d":
        cursor = collection_stations.update_many(geoquery, {"$set": {"status": False}})
    else:
        pass

if choix == 5:
    
    liste_staion = collection_vlilles.aggregate([
        {"$match":{"status": True}}, 
        {"$sort": {"record_timestamp": DESCENDING}}, 
        {"$match":{    
            "$or": [  
                { "$and" : [ { "record_timestamp" : { "$lte" : dateutil.parser.parse("2020-10-12 17:05:16.683Z")}} ,
                        { "record_timestamp" : { "$gte" : dateutil.parser.parse("2020-10-12 17:02:16.263Z")}} ]
                },{
                "$and" : [ { "record_timestamp" : { "$lte" : dateutil.parser.parse("2020-10-12 17:01:16.683Z")}} ,
                        { "record_timestamp" : { "$gte" : dateutil.parser.parse("2020-10-12 16:56:00.263Z")}} ]
                }
                ]
        }},
        {"$project":  
            {"_id":"$_id",
            "name": "$name",
                "total":{ "$add": ["$vlilles_dispo", "$places_dispo"]} , 
                "places_dispo" : "$places_dispo",
                "vlilles_dispo" : "$vlilles_dispo",
                "record_timestamp" : "$record_timestamp"
    }},
    {"$match":{"total": {"$gt": 0} }} ,   
    {"$project": 
            {"_id": "$_id", 
                "name": "$name", 
                "total": "$total", 
                "places_dispo" : "$places_dispo",
                "vlilles_dispo" : "$vlilles_dispo",  
                "percent" : {"$divide": [ "$vlilles_dispo" , "$total" ]},
                "record_timestamp" : "$record_timestamp"
    }},
    {"$match":{"percent": {"$lte": 0.2} }}, 
    {"$group": 
            {"_id":"$name",
            "entries" : {"$push" : {
                "percent": "$percent",
                "places_dispo" : "$places_dispo",
                "vlilles_dispo" : "$vlilles_dispo",
                "record_timestamp" : "$record_timestamp"}
    }}},
    {"$project": 
        { "_id":1 }},
    ])
    for station in liste_staion:
        print(station["_id"])
else:
    pass
