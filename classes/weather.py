#!/usr/bin/env python3 
#
# 1.01 10-APR-2019 xxxx 
# 

import re
import time
from datetime import datetime,timezone
import math
import sys
import os
import logging

import collections
import psycopg2




class weatherStation(object):
  
  def _get_stations(self,**postgres_params):
    # gets the details of all the stations from the psql database
    # for future use
    self.logger.debug("getting list of available weather stations from database")
    stations = {}
    try:
        pconn = psycopg2.connect(**postgres_params)
        pcur = pconn.cursor()
        self.logger.debug("PostgreSQL connection open") 
        pcur.execute("SELECT id,name, latitude, longitude, altitude, hardwarekey  FROM weather_station")
        stations = { col1:(col2, col3,col4,col5,col6) for (col1,col2,col3,col4,col5,col6) in pcur.fetchall()}
        
     	# close the communication with the PostgreSQL
        pconn.close()
        self.logger.debug("PostgreSQL connection closed")
    except (Exception, psycopg2.DatabaseError) as error:
        self.logger.error("PostgreSQL error")
        self.logger.error(error)
    finally:
        if pconn is not None:
            pconn.close()
            self.logger.debug('PostgreSQL connection tidied up')
    
    return stations 

  def _rak811_set_receive (self):
    ss = "at+set_config=lorap2p:transfer_mode:1\r\n"
    self.serial.write(bytes(ss,'utf-8'))
    response = self.serial.readline().decode('utf-8')
    self.logger.debug('_rak811_set_receive. Response: %s', response)
    return response

  def __init__(self, station_id, serial, **postgres_params):
    # to do - validate ststaion key as 16 characters
    
    self.VALID_MESSAGES = { 100 : "Weather Report", 
                            101 : "Station report",
                            200 : "Sync time",
                            201 : "Update station data"} # The last two are outgoing messages - 
                                                         # but we may get them from other base stations
                                                         
    
    self.logger = logging.getLogger(__name__)
    self.logger.addHandler(logging.NullHandler())
    self.logger.debug("initialising weatherStation object")
    
    self.stations = self._get_stations(**postgres_params)
    # take station id & find the hardware_key
    self.station_name, self.latitude, self.longitude, self.altitude, self.hardware_key = self.stations[int(station_id)]
    self.station_id = station_id
    
    self.serial = serial

    self._rak811_set_receive()
        

  def _rak811_set_send (self):
    ss = "at+set_config=lorap2p:transfer_mode:2\r\n"
    self.serial.write(bytes(ss,'utf-8'))
    response = self.serial.readline().decode('utf-8')
    self.logger.debug('rak811_set_send. Response: %s', response)
    return response
    
  def _rak811_send_data (self, message):
    
    #breakpoint()
    self.logger.debug("Sending message: %s", message)
    
    self._rak811_set_send()
    
    header = "at+send=lorap2p:" 
    crlf = "\r\n"
       
    self.serial.flush()
    
    self.serial.write(bytes((header+message+crlf),'utf-8'))
        
    response = self.serial.readline().decode('utf-8')
    
    self._rak811_set_receive()
    
    return response
  
  def send_data(self):
    
    # This is a '201' message type
    # C9|e6605481db318236|0050FA90|FFFD85D1|00BE
    # ^         ^            ^      ^        ^
    # message station key   lat     long    alt
    # number                        
    # Both latitude and longitude have five implied decimal places
    #     
    message_type = hex(201).replace('0x','')
    
    latitude_int = int(self.latitude * 100000)
    if latitude_int < 0:
      latitude_int = latitude_int + 4294967295 # This is a fudge to generate 2s complement for -ve latitude
    
    latitude_hex = hex(latitude_int).replace('0x','').zfill(8)
    

    longitude_int = int(self.longitude * 100000)
    if longitude_int < 0:
      longitude_int = longitude_int + 4294967295 # This is a fudge to generate 2s complement for -ve longitude
    
    longitude_hex = hex(longitude_int).replace('0x','').zfill(8)

    altitude = self.altitude 
    if altitude < 0:              # This is a fudge to generate 2s complement for -ve timezones
      altitude = altitude + 65536
    
    altitude_hex = hex(altitude).replace('0x','').zfill(4)
    
    message = message_type + self.hardware_key + latitude_hex + longitude_hex + altitude_hex 

    self._rak811_send_data(message)
  
  def sync_time(self):
    # This is a '200' message type
    # c8|e6605481db318236|38700f84|ffff
    # ^         ^            ^      ^
    # message station key   UTC     time offset
    # number                        (hours only)
    
    # get utc time

    message_type = hex(200).replace('0x','')

    time_now = datetime.now().astimezone()
    
    offset_seconds = time_now.utcoffset().total_seconds()
    
    # hours offset 
    offset = round ((offset_seconds)/3600)
    
    if offset < 0:              # This is a fudge to generate 2s complement for -ve timezones
      offset = offset + 65536
    
    #breakpoint()

    offset_hex = hex(offset).replace('0x','').zfill(4)

    #get another 'now' so we are synces as close as possible

    now_utc_hex = hex(round(datetime.now(timezone.utc).timestamp())).replace('0x','').zfill(8)

    breakpoint()
    message = message_type + self.hardware_key + now_utc_hex + offset_hex
    
    self._rak811_send_data(message)
  
  
  def listen(self):
    self._rak811_set_receive() # Just in case we are in the wrong mode
    rak811_data = ''
    
    while (rak811_data == ''):
      rak811_data = self.serial.readline().decode('utf-8')
    
    self.logger.debug('rak811_data: %s', rak811_data) 
    # need to check ID, really
    
    return rak811_data
  
  
  def parse_data(self, station_message):
    # define the splits in the data string
    
    # all message types
    MESSAGE_TYPE  = slice(0,2)
    HARDWARE_ID   = slice(2,18)
    EPOCH_TIME     = slice(18,26)
    TIMEZONE      = slice(26,30)

    # for weather report - id 100 (0x64)
    WIND_DIR      = slice(30,34)
    WIND_SPEED    = slice(34,38)
    WIND_GUST     = slice(38,42)
    WIND_GUST_DIR = slice(42,46)
    WIND_SPEED_2M = slice(46,50)
    WIND_DIR_2M   = slice(50,54)
    WIND_GUST_10M = slice(54,58)
    WIND_GUST_10M_DIRECTION = slice(58,62)
    HUMIDITY      = slice(62,66)
    TEMPERATURE   = slice(66,70)
    RAIN_1H       = slice(70,74)
    RAIN_TODAY    = slice(74,78)
    RAIN_SINCE_LAST = slice(78,82)
    BAR_UNCORRECTED = slice(82,90)
    BAR_CORRECTED   = slice(90,98)

    # for station_report - id 101 (0x65)
    LATITUDE = slice(30,38)
    LONGITUDE = slice(38,46)
    ALTITUDE  = slice(46,50)

    # split message into status (before the colon at+recv=-31,7,49) and the data
    # 64E6605481DB3182363 ....
  #            import pdb; pdb.set_trace()
    data_split = station_message.partition(":")  # now a tuple header, : , data
    data_hex = data_split[2]
    header = data_split[0].split("=",1)[1] #will give us a string '-58,7,49'
    RSSI, SNR, byte_count = header.split(",")
    
    if int(byte_count) > 49:
      self.logger.warning("Message is not recognised - ignoring:  %s", station_message)
    
       
    d = collections.OrderedDict() # for returning the data
    
    # following are for all message types
    d['RSSI'] = int(RSSI)
    d['SNR'] = int(SNR)
    d['bytes'] = int(byte_count)
    if d['bytes'] > 49:
      d['Unrecognised data'] = station_message
      self.logger.warning("Message is not recognised - stopping parse:  %s", station_message)
      return d
    
    message_type = int(data_hex[MESSAGE_TYPE],16) # convert from hex  

    if not message_type in self.VALID_MESSAGES:
      self.logger.warning("Message is not recognised - stopping parse:  %s", station_message)
      return d

    d['message_type'] = message_type
    d ['hardware_key'] = data_hex[HARDWARE_ID]
    d ['station'] = self.station_id 
    d ['epoch_time'] = int(data_hex[EPOCH_TIME],16)
    d ['timezone'] = int(data_hex[TIMEZONE],16)
        
    if d ['hardware_key'] != self.hardware_key:
      # rewrite station by looking up hardware key 
      sid = '' # temp copy of station id for lookup
      for key in self.stations:
        if self.stations[key][4] == d ['hardware_key']:
          sid = key
          break
      d['station'] = sid
      self.logger.warning("Message not for this basestation %s - message is for station id: %i", self.station_id, sid)

    # Generate a text timestamp including the time zones
    # The pico RTC is not tz aware so this has to be done 
    # manually (and carefully!). Only doing full hours at the moment

    ts = datetime.utcfromtimestamp(d['epoch_time'])
    string_time = (ts.strftime('%Y-%m-%d %H:%M:%S'))
    
    # add the sign
    if d['timezone'] < 0:
      string_time = string_time + '-'
    else:
      string_time = string_time + '+'

    string_time = string_time + str.zfill(str(d['timezone']),2) + ':00' # only doing whole hours

    d ['timestamp'] = string_time

    # process each of the message types

    if message_type == 100: # weather report 

      wind_direction = int(data_hex[WIND_DIR],16)
      if wind_direction > 0xcfff: # -ve number 2s complement
        wind_direction = -1 * (0xffff - wind_direction + 1) # can be -1 when wind sensor is detached

      d ['wind_direction'] = wind_direction

      d ['wind_speed'] = int(data_hex[WIND_SPEED],16)/100 # all but directions have 2 implied decimals 
      d ['wind_gust'] = int(data_hex[WIND_GUST],16)/100
      d ['wind_gust_dir'] = int(data_hex[WIND_GUST_DIR],16) 
      d ['wind_speed_avg2m'] = int(data_hex[WIND_SPEED_2M],16)/100
      d ['wind_dir_avg2m'] = int(data_hex[WIND_DIR_2M],16)
      d ['wind_gust_10m'] = int(data_hex[WIND_GUST_10M],16)/100
      d ['wind_gust_dir_10m'] = int(data_hex[WIND_GUST_10M_DIRECTION],16)
      d ['humidity'] = int(data_hex[HUMIDITY],16)/100
      temperature = int(data_hex[TEMPERATURE],16)
      
      if temperature > 0xcfff: # -ve number - 2s complement
        temperature = -1 * (0xffff - temperature + 1)
      d ['temperature'] = temperature/100 # 2 decimals
      
      d ['rain_1h'] = int(data_hex[RAIN_1H],16)/100
      d ['rain_since_last'] = int(data_hex[RAIN_SINCE_LAST],16)/100
      d ['rain_today'] = int(data_hex[RAIN_TODAY],16)/100
      d ['bar_uncorrected'] = int(data_hex[BAR_UNCORRECTED],16)/100
      d ['bar_corrected'] = int(data_hex[BAR_CORRECTED],16)/100
    
    elif message_type == 101: # station report
      
      latitude = int(data_hex[LATITUDE],16)
      if latitude > 0x7fffffff: #then this i a -ve number 
        latitude = -1 * (0xffffffff - latitude + 1) 
      d['latitude'] = latitude/100000 # 5 implied decimals
      
      longitude = int(data_hex[LONGITUDE],16)
      if longitude > 0x7fffffff: #then this i a -ve number 
        longitude = -1 * (0xffffffff - longitude + 1) 
      d['longitude'] = longitude/100000

      altitude = int(data_hex[ALTITUDE],16)
      if altitude > 0x7fff: # ive number
        altitude = -1 * (0xffff - altitude + 1 )
      d['altitude'] = altitude # no decimals

    else:
      
      self.logger.warning("Message type is not recognised: %i", message_type)
      #raise ValueError('unexpected message type: x' + data_hex[MESSAGE_TYPE])

    return d

    
  def commit_data(self,connection,data,station_id):
    
    if data['station'] != self.station_id:
      self.logger.warning("Message is not for this basestation - message for station id:%i", data['station'])
      self.logger.warning("Not committing data to database")
      return
    
    # only committing weather reports at moment
    if data['message_type'] != 100:
      self.logger.info("Ignoring message type: %i", data['message_type'])
      return

    cursor = connection.cursor()            
    
    pquery = ("INSERT INTO weather_reading ( "
                "reading_time, "
                "station_id, "
                "wind_dir, "
                "wind_speed, "
                "wind_gust, "         
                "wind_gust_dir, "     
                "wind_speed_avg2m, "  
                "wind_dir_avg2m, "
                "wind_gust_10m, "     
                "wind_gust_dir_10m, " 
                "humidity, "          
                "temperature, "       
                "rain_1h, "           
                "rain_today, "        
                "rain_since_last, "   
                "bar_uncorrected, "   
                "bar_corrected, "
                "battery, " 
                "light) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                          "%s, %s, %s, %s, %s, %s, %s, %s, %s)" )     

    self.logger.debug('postgreSQL query: %s', pquery) 

    pvalues = [ data['timestamp'], # reading_time
                station_id,
                data['wind_direction'], # wind_dir
                data['wind_speed'], # wind_speed
                data['wind_gust'], # wind_gust
                data['wind_gust_dir'], # wind_gust_dir
                data['wind_speed_avg2m'], # wind_speed_avg2m
                data['wind_dir_avg2m'], # wind_dir_avg2m
                data['wind_gust_10m'], # wind_gust_10m
                data['wind_gust_dir_10m'], # wind_gust_dir_10m
                data['humidity'], # humidity
                data['temperature'], # temperature
                data['rain_1h'],  # rain_1h
                data['rain_today'], # rain_today
                data['rain_since_last'], # rain_since_last
                data['bar_uncorrected'], # bar_uncorrected
                data['bar_corrected'], # bar_corrected
                0,             # battery
                0            ]  # light 
                
    try:
      self.logger.debug('Trying to update postgreSQL with query:')

      #print (pquery, pvalues)
      #breakpoint()
      cursor.execute (pquery, pvalues)

      connection.commit()

      count = cursor.rowcount
      
      self.logger.debug('%i record inserted into database.', count)

      self.logger.debug("Closing cursor")
      cursor.close()
      self.logger.debug("Cursor closed")

    except (Exception, psycopg2.DatabaseError) as error:
      self.logger.error("PostgreSQL error")
      self.logger.error(error)
    
    

