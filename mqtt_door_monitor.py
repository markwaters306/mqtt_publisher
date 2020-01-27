### csv and mqtt data logging ###
# import modules
import csv
from datetime import datetime
import datetime as dt
import os
import pandas as pd
import numpy as np
from gpiozero import Button
from signal import pause
import paho.mqtt.client as mqtt
import time

### Constants ###
LOG_DIR = '/home/pi/Documents/log_files/'
BASE_FILENAME = 'unit_log'
UNIT_TIME_STANDARD = 500
TIME_STANDARD_TOLERANCE = 10
OVER_TIME_STANARD_BAND_1 = 300
OVER_TIME_STANARD_BAND_2 = 1800
OVER_TIME_STANARD_BAND_3 = 3600
BROKER_NAME = 'raspberrypi'

#unit counter variable
unit_count = 0

# Init the input trigger
door_switch = Button(4)

def createRow(count,curr_time, time_diff,auto_time_class):
    """This function will accept a count value and a datetime value
        and will format it into a list"""
    unit_entry = [count,curr_time.year,curr_time.month,curr_time.day,\
    curr_time.hour,curr_time.minute,curr_time.second,\
    curr_time.timestamp(),time_diff.total_seconds(), \
    auto_time_class]
    return unit_entry

def writeRowToFile(filename, row):
    """This function will wtite a list as a csv row to
        file unit_log.csv in the wordking dir"""
    try:
        with open(filename,'a',newline='') as f:
            row_writer = csv.writer(f)
            row_writer.writerow(row)
    except:
        with open('error_log.csv','a', newline='') as error_f:
            print('Error writing to file')
            row_writer = csv.writer(error_f)
            row_writer.writerow(row)

def readLastUnitTime(filename):
    """This function will read the last row entry
        and will compare the time difference since the last unit"""
    try:
        df = pd.read_csv(filename, names=['Unit Number','Year,','Month',\
            'Day', 'Hour','Minute','Second','DateTime','Time Difference',\
            'Time Class'])
        #Get the last row of the data frame
        last_row = df.tail(1)
        seconds = last_row.iloc[0]['DateTime']
        #Convert float to datetime
        last_unit_time = datetime.utcfromtimestamp(seconds)
        # Get the last unit number        
        unit_count_local = int(last_row.iloc[0]['Unit Number']) + 1
    except:
        print("File Not accessible")
        last_unit_time = datetime.min           #set to 0
    return last_unit_time, unit_count_local

def calc_auto_time_class(time_diff):
    seconds = time_diff.total_seconds()
    #if the difference is 0 ignore the value
    if(seconds == 0):
        auto_time = -1
    #If unit was under time standard
    elif(seconds < UNIT_TIME_STANDARD - TIME_STANDARD_TOLERANCE):
        auto_time = 0
    #If unit was within ts tolerance
    elif(seconds < UNIT_TIME_STANDARD + TIME_STANDARD_TOLERANCE):
         auto_time = 1
    #if there was a delay of BAND 1
    elif(seconds < OVER_TIME_STANARD_BAND_1):
        auto_time = 2
    #if there was a delay of BAND 2
    elif(seconds < OVER_TIME_STANARD_BAND_2):
        auto_time = 3
    #if there was a delay of BAND 3
    elif(seconds < OVER_TIME_STANARD_BAND_3):
        auto_time = 4
    #If a larger delay than BAND3
    else:
        auto_time = 5
    return auto_time


def log_unit():
    curr_time=datetime.utcnow()                 # get the current time
    filename = "{}{}_{}.csv".format(LOG_DIR,BASE_FILENAME,curr_time.date())

    unit_count_local = 0
    # Check that a file exists for todays date
    if(os.path.isfile(filename)):
        # if it does exist get the time of the last unit 
        last_unit_time, unit_count_local = readLastUnitTime(filename)
        time_diff = curr_time - last_unit_time
    else:
        #If there is no existing record set the time difference to 0
        # and give a last unit time of 0
        print('creating new file...')
        last_unit_time = curr_time
        time_diff = curr_time - curr_time

    print("Unit {} Completed".format(unit_count_local))
    #Label the unit with a time class
    #This class cateorises the units into how long it took
    #between units
    auto_time_class = calc_auto_time_class(time_diff)

    # Check the date, if new day create new csv file
    #if last_unit_time.date != curr_time.date:
    #    # create new csv
    #    filename = "Unit_Log_{}.csv".format(curr_time.date)

    unit_entry = createRow(unit_count_local,curr_time,time_diff,auto_time_class)   
    writeRowToFile(filename,unit_entry)
    mqtt_client_publish(client,str(unit_entry))
    return

def mqtt_client_publish(client,data):
    print('connect to broker')
    client.connect(BROKER_NAME)
    client.loop_start()
    print('Subscribing to the topic','test/message')
    client.subscribe('test/message')
    print('Publishing message to the topic','test/message')
    client.publish('test/message',data)
    time.sleep(1)
    client.loop_stop()

while(True):
    print('creating new instance')
    client = mqtt.Client("P1")
    door_switch.when_pressed = log_unit
    print('connected')
    # Wait for a GPIO input     
    pause()
    
