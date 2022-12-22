import re
import time
from datetime import datetime, timezone
import math
import sys
import os
import logging
from urllib import request, parse
import collections
import psycopg2


import utilities as convert

class metofficeWow(object):

    def _get_stations(self, **postgres_params):
    # gets the details of all the stations from the psql database
    # for future use
        self.logger.debug(
            "getting list of available weather stations from database")
        stations = {}
        try:
            pconn = psycopg2.connect(**postgres_params)
            pcur = pconn.cursor()
            self.logger.debug("PostgreSQL connection open")
            pcur.execute(
                "SELECT id,name, wow_station, wow_key FROM weather_station"
            )
           
            stations = {
                col1: (col2, col3, col4)
                for (col1, col2, col3, col4) in pcur.fetchall()
            }

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

    def __init__(self, station_id,  wow_params, **postgres_params):
        # to do - validate ststaion key as 16 characters

        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(logging.NullHandler())
        self.logger.debug("initialising weatherStation object")

        self.stations = self._get_stations(**postgres_params)
        self.station_id = station_id

        self.station_data = self.stations[station_id]

        self.postgres_params = postgres_params
        
        self.software = wow_params['software']
        self.upload_url = wow_params['upload_url']
        
        self.logger.debug("metofficeWow initialised")

    def latest(self, upload=False):
        self.logger.debug("Fetching latest reading")
        
        # List of available fields for wow uploads and local database equivalent
        # **********************************************************************
        #
        # name          units               our field equivalent (units)
        #============   ===============     ============================
        # baromin       Inch of Mercury     bar_uncorrected (Hpa) 
        # dailyrainin	Inches              rain_today (mm)
        # dewptf		Fahrenheit          <not collected>
        # humidity		0-100 %             humidity (%) - limit to 100
        # rainin	    Inches              rain_since_last(mm) - needs summing
        # soilmoisture	0-100 %             <not collected>
        # soiltempf 	Fahrenheit          <not collected>
        # tempf		    Fahrenheit          temperature(Celsius)
        # visibility	Kilometres          <not collected>
        # winddir		Degrees (0-360)     wind_dir(degrees)
        # windspeedmph	Miles per Hour      wind_speed (km/h)
        # windgustdir	0-360 degrees       wind_gust_dir_10m (km/h)
        # windgustmph   degrees             wind_gust_10m (km/h)
        
        
        
        
        try:
            pconn = psycopg2.connect(**self.postgres_params)
            pcur = pconn.cursor()
            self.logger.debug("PostgreSQL connection open")
            
            query = 'select reading_time, bar_uncorrected, rain_today, temperature, humidity, ' \
                    'wind_dir, wind_speed, wind_gust_10m, wind_gust_dir_10m ' \
                     'from weather_reading where station_id = ' + str(self.station_id) + ' order by reading_time desc limit 1;'
            
            pcur.execute(query)
            
            columns = pcur.description 
            result = [{columns[index][0]:column for index, column in enumerate(value)} for value in pcur.fetchall()] [0]
            
            reading_time = result['reading_time'] # will need later

            # result is now a dictionary of field name/ value pairs - change the db field names for the met office ones
            # and convert the units as well

            result ['dateutc'] = result.pop('reading_time').replace(tzinfo=None)
            result ['baromin'] = convert.hpa_to_inches(result.pop('bar_uncorrected')) 
            result ['dailyrainin'] = convert.mm_to_inches(result.pop('rain_today'))
            result ['humidity'] = convert.limit_percent(result.pop('humidity'))
            result ['tempf'] = convert.celsius_to_f(result.pop('temperature'))
            result ['winddir'] = result.pop('wind_dir')
            result ['windspeedmph'] = convert.kph_to_mph(result.pop('wind_speed'))
            result ['windgustdir'] = result.pop('wind_gust_dir_10m')
            result ['windgustmph'] = convert.kph_to_mph(result.pop('wind_gust_10m'))
            result ['softwaretype'] = self.software

            result['siteid'] = self.station_data[1]
            result['siteAuthenticationKey'] = self.station_data[2]
            
            query_url = self.upload_url + parse.urlencode(result)

            if upload: 
                self.logger.debug("Preparing to update last upload time")
                u = request.urlopen(query_url)
                response = u.read()
                query = 'UPDATE weather_station set wow_last_upload = (%s) ' \
                        'WHERE id = (%s);'
                pcur.execute(query,(reading_time, self.station_id))
                pconn.commit()
                self.logger.debug("Last upload time updated")
            else:
                self.logger.info('url generated: %s', query_url)

            #breakpoint()
            
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
    