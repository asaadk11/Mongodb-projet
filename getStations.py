import requests
import json
from pprint import pprint
from pymongo import MongoClient


def get_velib(url):
    payload = {}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])


vlilles = get_velib(
    "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=-1")


vlilles_format = []
for vlib in vlilles:
    vlilles_format.append({
        "name": vlib["fields"]["nom"],
        "city": vlib["fields"]["commune"],
        "size": vlib["fields"]["nbvelosdispo"] + vlib["fields"]["nbplacesdispo"],
        "geo": vlib["geometry"],
        "TPE ": vlib["fields"]["type"] != "SANS TPE",
        "status": vlib["fields"]["etat"] == "EN SERVICE",
        "last update": vlib["record_timestamp"] })



print("Lille : " + str(len(vlilles_format)))

client = MongoClient('mongodb+srv://admin:admin@cluster0.8mptla5.mongodb.net/test')
db = client.vls  
collection = db.stations  
collection.delete_many({})

print("inserted : " + str(len(collection.insert_many(vlilles_format).inserted_ids)))
