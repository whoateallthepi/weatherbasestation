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

import io, os, sys

from config import config # used to read the config file for postgreql - database.ini

from classes.weather import weatherStation

version = '1.dev.00'

# do the arguments

parser = argparse.ArgumentParser()

parser.add_argument("--tty",help="serial termninal to connect to lora",
                    default="/dev/ttyUSB0")

parser.add_argument("--debug", help="helps us debug",
                    action="store_true")

parser.add_argument("--interactive", help="Prompt for action rather than the loop",
                    action="store_true")

parser.add_argument("--baud", help="serial port baud. Default 115200",
                    default=115200)
                    
parser.add_argument("--station", help="Id of the station - default is '04'",
                   default='4')

parser.add_argument("--update_time", help="Send the current basestation time to the weather station (message type 200).",
                    action="store_true")
                    
parser.add_argument("--update_station", help="Send station details (name, location, altitude) to weather station.",
                    action="store_true")

parser.add_argument('--station_key', help="Hardware key of the remote weather station. Default e660583883265039",
                     default="e660583883265039")

parser.add_argument("--log_file", help="Location of log file - defauits to /var/log/weather.log",
                    default = "/var/log/weather.log")

parser.add_argument("--log", help="log level - suggest <info> when it is working",
                    default="DEBUG")

postgres_params = config()

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

logger = logging.getLogger(('getweather.py: ' + version))

if args.debug:
    console_handler = logging.StreamHandler()
    logger.addHandler(console_handler)
    logger.debug('Running in debug mode')

logger.info('Starting')

if args.debug:
    logger.info("Debugging mode")

postgres_params = config()

port = args.tty
baud = args.baud
timeout = 10.0
debug = args.debug
station = args.station_key

testing = False
#testing = True



def open_serial():
    logger.debug("Opening serial interface")

    ser = serial.Serial(port,baudrate=baud, 
                        timeout=timeout, 
                        writeTimeout=timeout, 
                        )

    return ser
      
def run_interactive(serial):
    # clear any crap from the seial link
    # discard = serial.readline().decode('utf-8')
    
    wstation = weatherStation(args.station, serial, **postgres_params)
    
    while True:
        main_prompt = 'L(isten), listen and (C)ommit, send (T)ime, update station (D)etails, (Q)uit: '
        action = input(main_prompt)
        if (action.upper() == 'L' or action.upper() == 'C'):
            while True:
                data = wstation.listen()
                pd = wstation.parse_data(data)
                print(pd)
                
                if action.upper() == 'C':
                    logger.info("Connecting to PostgreSQL server")
                    try:
                        pconn = psycopg2.connect(**postgres_params)
                    except (Exception, psycopg2.DatabaseError) as error:
                        logger.error("Failed to connect to PostgreSQL")
                        logger.error(error)
                
                    pcur = pconn.cursor()
                    wstation.commit_data(pcur,pd,4)

                    logger.debug("Closing connection to PostgreSQL server")
                    pcur.close()
                    logger.debug("PostgreSQL connection closed")
                    
        elif action.upper() == 'T':
            wstation.sync_time()
        elif action.upper() == 'D':
            wstation.send_data()
        elif action.upper() == 'Q':
            exit() 
        else:
            print('Error - unrecognised action')       
         
         

    
# ------------------------------------------------------Main processing starts here ---------------------------------------------------------#

    
def main():     

     
    #breakpoint()
    
    logger.debug('basestation.py %s', version)
    ser = open_serial()
       
    # station_name, latitude, longitude, altitude = get_station(station_id)
    at="at+version\r\n"
    
    ser.flush()
    ser.write(bytes(at,'utf-8'))
    
    response = ser.readline().decode('utf-8')
    
    print('RAK811 version:', response)
    
    if args.interactive:
        run_interactive(ser)
        return
    else:
        return    
        
    

if __name__ == '__main__':
    logger.debug('About to drop into main()')
    main()
