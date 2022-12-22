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
import re

# used to read the config file for postgreql - database.ini
from config import config_new
from config import config_wow

from classes.weatherObservation import metofficeWow


version = '1.dev.1'

parser = argparse.ArgumentParser()


parser.add_argument("--debug", help="helps us debug",
                    action="store_true")

parser.add_argument("--update", help= "Specify to actually send reading to met office, otherwise just print out",
                    action="store_true")  

parser.add_argument("--station", help="Id of the station - default is '05'",
                   default='5')

parser.add_argument("--log_file", help="Location of log file - defauits to ''",
                    default="/var/log/lora_basestation.log")

parser.add_argument("--log", help="log level - suggest <info> when it is working",
                    default="DEBUG")

postgres_params = config_new()
wow_params = config_wow()

args = parser.parse_args()

loglevel = args.log
update = args.update

numeric_level = getattr(logging, loglevel.upper(), None)

if not isinstance(numeric_level, int):
    raise ValueError('Invalid log level: %s' % loglevel)

# initialise logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%d/%m/%Y %H:%M:%S',
    filename=args.log_file,
    level=numeric_level)

logger = logging.getLogger(('basestation_wow.py: ' + version))

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

if args.update:
    logger.info("Will send update to met office wow")
else:
    logger.info("Not sending update to met office wow")

debug = args.debug

station_id = int(args.station)

def main():
    logger.info('basestation_wow.py %s', version)

    wow = metofficeWow(station_id, wow_params,**postgres_params)

    wow.latest(upload=args.update)

    logger.info('Exiting basestation_wow.py')

    

if __name__ == '__main__':
    logger.debug('About to drop into main()')
    main()

