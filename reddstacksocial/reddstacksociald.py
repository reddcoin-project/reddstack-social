import json
import time
import os
import logging
from logging.handlers import RotatingFileHandler
from pymongo import MongoClient
from blockstore_client import config, client as clientRS, schemas, parsing, user, storage, drivers
log = config.log

#Remove all logging handlers and pipe everything to file
for handler in log.handlers[:]:
    log.removeHandler(handler)

DEBUG = True
logPath = "../logs"
if not os.path.isdir(logPath):
    os.makedirs(logPath)
logFileHandler = RotatingFileHandler(logPath + "/reddstack-social.log", maxBytes=50000000, backupCount=99)
log_format = ('[%(asctime)s] [%(levelname)s] [%(module)s:%(lineno)d] (' + str(os.getpid()) + ') %(message)s' if DEBUG else '%(message)s')
logfile_formatter = logging.Formatter(log_format)
logFileHandler.setFormatter(logfile_formatter)
log.addHandler(logFileHandler)

# processing delay
delayTime = 10 # sec

# Setup blockstore connection
conf = config.get_config()
conf["network"] = "mainnet"
proxyRS = clientRS.session(conf, conf['server'], conf['port'])

clientDB = MongoClient('localhost', 27017)
db = clientDB['socialAccounts']
# collections
registrationColls = db.registrations
networkColls = db.networks


def getNameRecord(name):
    try:
        record = clientRS.get_name_blockchain_record(name)
    except Exception as e:
        log.error(e)
        return None
    return record


def getAllNames():
    try:
        record = clientRS.get_all_names(None, None)
    except Exception as e:
        log.error(e)
        return None
    return record


def getNetworks(name, hash):
    try:
        record = clientRS.get_immutable(name, hash)
    except Exception as e:
        log.error(e)
        return None
    if 'error' not in record:
        return record['data']
    else:
        return json.dumps(record)

def run_sweep():

    all_names = getAllNames()

    if all_names is not None:

        if "error" not in all_names:
            if "bitcoind_blocks" not in all_names:
                for each in all_names:
                    # rate limit our queries
                    startTime = time.time()

                    log.info("Checking: " + str(each))
                    user = all_names[each]
                    # add a record for every user
                    # check exist first
                    username = user["name"].encode('ascii')
                    username = username.split(".")
                    results = registrationColls.find({username[1] + "." + username[0]: {'$exists': True}}).count()
                    if results == 0:
                        # No matches, we can go ahead and add
                        log.info(str(results) + " match(es) found in DB, adding.." + username[0])
                        # add the minimal needed info {network:userid:address:value_hash)
                        addUser = {username[1]: {username[0]: {"value_hash": user["value_hash"], "address": user["address"]}}}
                        result = registrationColls.insert_one(addUser)
                        log.info('One User Added: {0} {1}'.format(result.inserted_id, user["name"]))

                    elif results == 1:
                        # One matches, we can go ahead and add
                        log.info(str(results) + " match(es) found in DB, updating.." + username[0])

                        # ,"networks":{"reddit":{"cryptognasher":"address"},"twitter":{"gnasher":"address"}}}
                        # get registered results
                        queryRegUser = registrationColls.find({username[1] + "." + username[0]: {'$exists': True}})
                        storedHash = queryRegUser[0][username[1]][username[0]]["value_hash"]

                        if "value_hash" in user:
                            if user["value_hash"] is not None:  # no social data
                                #print user["name"] + " " + user["value_hash"]
                                # if different values, something has been updated
                                # No matches, we can go ahead and add
                                if storedHash != user["value_hash"]:
                                    log.info("Exactly " + str(results) + " match found in DB, updating " + username[0] + " if required..")
                                    log.info(user["name"] + " " + user["value_hash"])
                                    #lookup = clientRS.get_name_record(user["name"])

                                    networksData = getNetworks(user["name"], user["value_hash"])
                                    networksData = json.loads(networksData)
                                    #networksData = {"v": 2, "name": {"formatted": "gnasher0012"}, "networks": {"reddit": {"username": "crypto888", "proofURL": "", "address": ""}, "twitter": {"username": "gnash0000", "proofURL": "", "address": ""}}}

                                    if 'error' not in networksData:
                                        if networksData is not None:
                                            log.info( networksData['networks'])
                                            networksT = networksData["networks"]
                                            for net in networksT:
                                                log.info( networksT[net] )
                                                if "username" in networksT[net]:
                                                    uid = networksT[net]["username"].encode("ascii")
                                                    queryNetwork = networkColls.find({net.encode('ascii') + ".username": uid}).count()
                                                    if queryNetwork == 0:
                                                        log.info("Need to insert record")
                                                        addUser = {net: {"username": networksT[net]["username"], "proofURL": networksT[net]["proofURL"], "address": networksT[net]["address"], "fingerprint":networksT[net]["fingerprint"]}}
                                                        result = networkColls.insert_one(addUser)
                                                        log.info(result)
                                                    elif queryNetwork == 1:
                                                        log.info("Need to update record: %s" % networksT[net]["username"])
                                                        updateUser = {net + '.username': networksT[net]["username"]}
                                                        updatePayload = {'$set':{net + ".proofURL": networksT[net]["proofURL"], net + ".address": networksT[net]["address"], net + ".fingerprint":networksT[net]["fingerprint"]}}
                                                        result = networkColls.update_one(updateUser, updatePayload, True)
                                                        log.info("Updated {0} records.".format(result.modified_count))
                                else:
                                    # No matches, we can go ahead and add
                                    log.info("Stored hash: {0} equal current hash: {1}. Skipping".format(storedHash,user['value_hash']))
                            else:
                                log.info("value_hash is None. Nothing to update")

                    endTime = time.time()
                    remainingTime = startTime + delayTime - endTime
                    if remainingTime > 0:
                        log.info("Delaying next query {0} secs".format(remainingTime))
                        time.sleep(remainingTime)
            else:
                log.error("Some other data")
        else:
            log.error(all_names["error"])

def run_reddstacksociald():
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

