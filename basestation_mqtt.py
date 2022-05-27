#!/usr/bin/env python3

import serial
import time
#import datetime
from datetime import datetime
from datetime import timedelta
import pytz
import math
import argparse
import logging
from time import sleep
import binascii
import psycopg2

from configparser import ConfigParser

import io
import os
import sys

# used to read the config file for postgreql - database.ini
from config import config_new
from config import config_mqtt

from classes.thingsNetwork import weatherStation

from classes.thingsNetwork import ttnMQTT

version = '1.dev.00'

# do the arguments

parser = argparse.ArgumentParser()


parser.add_argument("--debug", help="helps us debug",
                    action="store_true")

parser.add_argument("--interactive", help="Prompt for action rather than the loop",
                    action="store_true")

parser.add_argument("--station", help="Id of the station - default is '05'. Only matters for downlinks",
                   default='5')

parser.add_argument("--action", help ="L(isten), listen and (C)ommit,"  +
                    " update station (D)etails, (R)eboot, (Q)uit. Batch mode only",
                    default = 'l')

parser.add_argument("--update_time", help="Send the current basestation timezone to the weather station (message type 200).",
                    action="store_true")

parser.add_argument("--update_station", help="Send station details (name, location, altitude) to weather station.",
                    action="store_true")

# parser.add_argument('--station_key', help="Hardware key of the remote weather station. Default e660583883265039",
#                     default="e660583883265039")

parser.add_argument("--log_file", help="Location of log file - defauits to ''",
                    default="/var/log/lora_basestation.log")

parser.add_argument("--log", help="log level - suggest <info> when it is working",
                    default="DEBUG")

postgres_params = config_new()

mqtt_params = config_mqtt()

args = parser.parse_args()

loglevel = args.log

numeric_level = getattr(logging, loglevel.upper(), None)

if not isinstance(numeric_level, int):
    raise ValueError('Invalid log level: %s' % loglevel)

# initialise logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%d/%m/%Y %H:%M:%S',
    filename=args.log_file,
    level=numeric_level)

logger = logging.getLogger(('basestation.py: ' + version))

if args.debug:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    # simpler formatter for console
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console_handler.setFormatter(formatter)
    logging.getLogger('').addHandler(console_handler)
    # logger.addHandler(console_handler)
    logger.debug('Running in debug mode')
    
logger.info('Starting')

if args.debug:
    logger.info("Debugging mode")


debug = args.debug


testing = False
#testing = True

station_id = int(args.station)




# ------------------------------------------------------Main processing starts here ---------------------------------------------------------#


def main():

    logger.debug('basestation_mqtt.py %s', version)

    if args.interactive:
        logger.info("Running in interactive mode")
        main_prompt = 'L(isten), listen and (C)ommit, send (T)ime, update station (D)etails, (R)eboot, (Q)uit: '
        action = input(main_prompt)
    else:
        logger.info("Running in batch mode")
        action = args.action
    
    if (action.upper() == 'L' or action.upper() == 'C'):
        commit = (action.upper() == 'C')
        ws = weatherStation(station_id,**postgres_params)
        mqttc = ttnMQTT(ws, commit, **mqtt_params)
        mqttc.process_link(direction = 'UPLINK') # runs forever UPLINKs are device > TTN
        
    elif action.upper() == 'T':
        ws = weatherStation(station_id, **postgres_params)
        offset_message = ws.sync_time()
        mqttc = ttnMQTT(ws, False, **mqtt_params)
        mqttc.process_link(direction = 'DOWNLINK', data = offset_message)
        
    elif action.upper() == 'D':
        ws = weatherStation(station_id,**postgres_params)
        stationdata_message = ws.send_data()
        mqttc = ttnMQTT(ws, False, **mqtt_params)
        mqttc.process_link(direction = 'DOWNLINK', data = stationdata_message)
    
    elif action.upper() == 'R':
        ws = weatherStation(station_id,**postgres_params)
        reboot_message = ws.reboot()
        mqttc = ttnMQTT(ws, False, **mqtt_params)
        mqttc.process_link(direction = 'DOWNLINK', data = reboot_message)

    elif action.upper() == 'Q':
        exit()
    else:
        print('Error - unrecognised action')
     


if __name__ == '__main__':
    logger.debug('About to drop into main()')
    main()
