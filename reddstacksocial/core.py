#!/usr/bin/env python
# coding=utf-8

from __future__ import unicode_literals
import json
import time
import config
from pymongo import MongoClient
from twisted.internet.defer import inlineCallbacks, returnValue
from blockstore_client import config as bc_config, client

log = config.log


# processing delay
delayTime = 1 # sec

# Setup blockstore connection
conf = bc_config.get_config()
conf["network"] = "mainnet"
proxyRS = client.session(conf, conf['server'], conf['port'])

clientDB = MongoClient('localhost', 27017)
db = clientDB['socialNetworks']
# collections
reddids_colls = db.reddids
network_colls = db.networks

network_sites = ['reddit', 'youtube', 'facebook', 'twitter']

@inlineCallbacks
def getNameRecord(name):
    try:
        record = yield client.get_name_blockchain_record(name)
    except Exception as e:
        log.error(e)
        returnValue(None)
    returnValue(record)

@inlineCallbacks
def getAllNames():
    try:
        record = yield client.get_all_names(None, None)
    except Exception as e:
        log.error(e)
        returnValue(None)
    returnValue(record)

@inlineCallbacks
def getNetworks(name, hash):
    try:
        record = yield client.get_immutable(name, hash)
    except Exception as e:
        log.error(e)
        returnValue(None)
    if 'error' not in record:
        returnValue(record['data'])
    else:
        returnValue(record)

def split_name(name):
    username = name["name"].encode('ascii')
    return username.split(".")

def get_dbuser(name):
    username = name["name"].encode('ascii')
    uid, namespace = username.split(".")

    # get current registered results
    queryRegUser = reddids_colls.find({'name': uid, 'namespace': namespace})
    return queryRegUser[0]


def get_dbuserhash(details):
    return details["value_hash"]


def add_dbuser(name):
    # add the minimal needed info {network:userid:address:value_hash)
    log.info("Adding: " + str(name['name']))
    username = name["name"].encode('ascii')
    uid, namespace = username.split(".")

    adduser = {"namespace": namespace, "name": uid, "value_hash": name["value_hash"], "address": name["address"], "networks": []}
    result = reddids_colls.insert_one(adduser)
    log.info('One User Added: {0} {1}'.format(result.inserted_id, name["name"]))
    return result.inserted_id


def check_db_entry(name):
    log.info("Checking: " + str(name['name']))

    # add a record for every user
    # check exist first
    username = name["name"].encode('ascii')
    uid, namespace = username.split(".")
    return reddids_colls.find({"name":uid, "namespace":namespace}).count()

def insert_social_network(uid, network, networkData):
    addUser = {
        "network": network,
        "username": networkData["username"],
        "proofURL": networkData["proofURL"],
        "address": networkData["address"],
        "fingerprint": networkData["fingerprint"],
        "valid": "",
        "owner": uid
    }
    result = network_colls.insert_one(addUser)
    log.info("Inserted record {0} for {1} with result {1}".format(network, networkData["username"], result))

    return result.inserted_id

def update_social_network(uid, network, networkData):
    updateUser = {"owner": uid, "network": network}
    updatePayload = {'$set':
        {
        "network": network,
        "username": networkData["username"],
        "proofURL": networkData["proofURL"],
        "address": networkData["address"],
        "fingerprint": networkData["fingerprint"],
        "valid": "",
        "owner":uid
        }
    }

    result = network_colls.update_one(updateUser, updatePayload, upsert=True)

    log.info("Inserted record {0} for {1} with result {1}".format(network, networkData["username"], result))

    return result

def insert_social_networks(uid, profileData):
    log.info("Inserting Networks: {0}".format(uid))
    networks = profileData["networks"]

    results = []

    for network in networks:
        if network in network_sites:
            log.info("Inserting: {0} for {1}".format(network, uid))

            result = insert_social_network(uid, network, networks[network])

            results.append(str(result))

    return results


def update_social_networks(uid, profileData):
    log.info("Updating Networks: {0}".format(uid))
    networks = profileData["networks"]

    results = []

    for network in networks:
        if network in network_sites:
            log.info("Updating: {0} for {1}".format(network, uid))

            result = update_social_network(uid, network, networks[network])

            if result.upserted_id is not None:
                results.append(str(result.upserted_id))

    return results

def update_user_with_network_ids(uid, results):
    log.info("Updating: {0}".format(uid))
    name, namespace = uid.split(".")

    updateUser = {"name": name, "namespace": namespace}
    updatePayload = {'$set': {"networks": results}}
    result = reddids_colls.update_one(updateUser, updatePayload)

    return result


def update_user_hash(uid, hash, results):
    log.info("Updating: {0}".format(uid))
    name, namespace = uid.split(".")

    updateUser = {"name": name, "namespace": namespace}
    updatePayload = {'$set': {"value_hash": hash, "networks": results}}
    result = reddids_colls.update_one(updateUser, updatePayload)

    return result


def process_names(names):

    try:
        if "error" not in names:
            for name in names:
                user = names[name]

                # get the number of db entries
                result = check_db_entry(user)
                if result == 0:
                    # No matches, we can go ahead and add this one to the db
                    log.info("{0} match(es) found in DB, adding..{1}".format(result, name))
                    adduser_result_id = add_dbuser(user)

                    # if there is a value_hash available and we have just inserted a user
                    if user['value_hash'] is not None:
                        networksData = getNetworks(user["name"], user["value_hash"])
                        if 'error' not in networksData.result:
                            add_networks_results  = insert_social_networks(user["name"], json.loads(networksData.result))
                            if len(add_networks_results) > 0:
                                update_user_with_network_ids_result = update_user_with_network_ids(user['name'], add_networks_results)
                        else:
                            log.info('Error {0}'.format(networksData.result))



                elif result == 1:
                    # One match found, do we need to update the db
                    log.info("{0} match(es) found in DB, do we need to update..{1}?".format(result, name))
                    dbuser = get_dbuser(names[name])
                    storedhash = get_dbuserhash(dbuser)

                    if storedhash != user['value_hash']:
                        # Need to update user, set social networks
                        log.info("User: {0} DB Hash: {1} <> Blockchain Hash: {2}".format(user["name"], storedhash, user["value_hash"]))
                        networksData = getNetworks(user["name"], user["value_hash"])
                        if "error" not in networksData.result and networksData.result is not None:
                            #update the network profiles, return list of objects
                            results = update_social_networks(user["name"],json.loads(networksData.result))

                            # update the 'value_hash' and the social network objects
                            update_user_hash(user["name"], user["value_hash"], results)

                    else:
                        log.info('No updates required')


                else:
                    # More than one match found, we might need to modify the db
                    log.info("{0} match(es) found in DB, checking..".format(result, name))

        else:
            log.error(names["error"])

    except Exception as e:
        log.error("Exception occurred:\n%s" % e)
        return





def run_sweep():

    try:
        all_names = getAllNames()
    except Exception as e:
        log.error("Exception occurred:\n%s" % e)
        return

    if all_names is not None:
        process_names(all_names.result)


def run():
    log.info("\n\n**************************\n Starting Reddstack Social\n**************************\n")
    while True:
        startTime = time.time()
        try:
            run_sweep()
        except Exception as e:
            log.error ("Exception occured:\n%s" % e)
        finally:
            endTime = time.time()
            processingTime = endTime - startTime
            log.info(" Processing Time = {0}".format(processingTime))
            log.info(" Sleeping 60 sec")
            time.sleep(60)

