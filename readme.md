# An Pi/Python example of a receiving basestation for an openaws Pico weather station
## Instalation 
This is where you were hoping to find some helpful instructions
## Overview
This is a sister project for https://github.com/whoateallthepi/picoweatherstation The latter is a functioning weather station sending out regular reports via a loraWAN network. This is a receiving station wth some basic receiver functionality - decoding the data, storing in a database etc. It also can send messages to update the details on the weather station head end (altitude is particularly important), and also to sync the timeone between the base station and the weather station.

Base station to weather station message-types are in the range 200-255. Weather station to base station message types are in the range 100-199. The full message structure is defined in the weather station documentation https://github.com/whoateallthepi/picoweatherstation#readme 

As of this version the original basestation.py code has been superceded by basestation_mqtt.py With the head end switching to loraWAN (from lora p2p) we no longer ommunicate directly with the modem via the serial port. Messages from the station (uplinks in loraWAN speak) are received via a Mosquitto connection to The Things Network. Downlinks are sent via a different Mosquitto connection. 

### Currently implemented message types

| Message number | Hex | Details | Direction |
| -------------- | --- | ------- | --------- |
| 100            | x64 | Contains the latest weather readings | weather station > base station |
| 101            | x65 | Details of station including date, time, altitude and position | weather station > base station |
| .. |
| 200            | xc8 | Sends the current base station timezone to weather station for syncing, plus a seconds adjutment for the clock (-127 to +127 ) | downlink base station > weather station |
| 201            | xc9 | Sends the station details - altitude, position - to the weather tation |  downlink |
| 202            | xca | Requests station to send a type 101 message at next opportunity | downlink |
| 203            | xcb | Requests a software reboot of the weather station  (via the watchdog ) | downlink |

### Note on hardware keys
With the switch to loraWAN, the hardware key of the pico board is no longer significant. 

## Database  
## Hardware build
With the witch to loraWAN, there are no longer any hardware build requirements.
## Receiving messages
## Sending messages