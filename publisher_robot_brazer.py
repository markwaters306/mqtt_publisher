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
import argparse
import socket

### Constants ###
BASE_FILENAME = 'unit_log'
UNIT_TIME_STANDARD = 50
EMPTY_ROTATION = 2
TIME_STANDARD_TOLERANCE = 14
OVER_TIME_STANARD_BAND_1 = 100
OVER_TIME_STANARD_BAND_2 = 900
OVER_TIME_STANARD_BAND_3 = 1800
BROKER_NAME = '192.168.0.100'

def createRowFlame(curr_time, flame_status):
    """
    This funtion will accept a current time and flame staus Bool
    It wil create a row to send over MQTT and save to csv
    """
    row_entry = [curr_time.year,curr_time.month,curr_time.day,\
    curr_time.hour,curr_time.minute,curr_time.second,\
    curr_time.timestamp(),flame_status]
    return row_entry

def createRow(count,curr_time, time_diff,auto_time_class):
    """
    This function will accept a count value and a datetime value
    and will format it into a list
    """
    unit_entry = [count,curr_time.year,curr_time.month,curr_time.day,\
    curr_time.hour,curr_time.minute,curr_time.second,\
    curr_time.timestamp(),time_diff.total_seconds(), \
    auto_time_class]
    return unit_entry

def writeRowToFile(filename, row):
    """This function will write a list as a csv row to
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
    #If the time recorded was under a minute it is likely
    #That the table is being rotated empty and should be ignored
    elif(seconds < EMPTY_ROTATION):
        auto_time = 0
    #If unit was under time standard
    elif(seconds < UNIT_TIME_STANDARD - TIME_STANDARD_TOLERANCE):
        auto_time = 1
    #If unit was within ts tolerance
    elif(seconds < UNIT_TIME_STANDARD + TIME_STANDARD_TOLERANCE):
         auto_time = 2
    #if there was a delay of BAND 1
    elif(seconds < OVER_TIME_STANARD_BAND_1):
        auto_time = 3
    #if there was a delay of BAND 2
    elif(seconds < OVER_TIME_STANARD_BAND_2):
        auto_time = 4
    #if there was a delay of BAND 3
    elif(seconds < OVER_TIME_STANARD_BAND_3):
        auto_time = 5
    #If a larger delay than BAND3
    else:
        auto_time = 6
    return auto_time

def log_flame():
    

def log_door():
    curr_time=datetime.utcnow()                 # get the current time
    # Create a file name for the door trigger log
    filename = "{}{}_door_{}.csv".format(log_dir,BASE_FILENAME,curr_time.date())

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

    #Label the unit with a time class
    #This class cateorises the units into how long it took
    #between units
    auto_time_class = calc_auto_time_class(time_diff)
    
    #set the topic to publish to
    door_topic = '{}/door'.format(topic)
    
    # Save the info and publish to mqqt broker
    # If the time class is negative don't save the unit
    if(auto_time_class != 0):
        unit_entry = createRow(unit_count_local,curr_time,time_diff,auto_time_class)   
        writeRowToFile(filename,unit_entry)
        print("Door Trigger Count: {} ".format(unit_count_local))
        mqtt_client_publish(client,door_topic,str(unit_entry))    
    else:
        print("Switch Bounced: Ignoring Trigger")
    return

def log_braze():
    curr_time=datetime.utcnow()                 # get the current time
    filename = "{}{}_braze_{}.csv".format(log_dir,BASE_FILENAME,curr_time.date())

    unit_count_local = 0
    # Check that a file exists for todays date
    if(os.path.isfile(filename)):
        # if it does exist get the time of the last unit 
        last_unit_time, unit_count_local = readLastUnitTime(filename)
        time_diff = curr_time - last_unit_time
    else:
        #If there is no existing record set the time difference to 0
        # and give a last unit time of 0
        print('Creating new file...')
        last_unit_time = curr_time
        time_diff = curr_time - curr_time

    #Label the unit with a time class
    #This class cateorises the units into how long it took
    #between units
    auto_time_class = calc_auto_time_class(time_diff)
    
    #set the topic to publish to
    braze_topic = '{}/braze'.format(topic)
    
    # Save the info and publish to mqqt broker
    # If the time class is negative don't save the unit
    if(auto_time_class != 0):
        unit_entry = createRow(unit_count_local,curr_time,time_diff,auto_time_class)   
        writeRowToFile(filename,unit_entry)
        print("Braze Trigger Count: {} ".format(unit_count_local))
        mqtt_client_publish(client,braze_topic,str(unit_entry))    
    else:
        print("Switch Bounced: Ignoring Trigger")
    return

def mqtt_client_publish(client,topic,data):
    print('Connect to broker')
    try:
        client.connect(broker_name)
        client.loop_start()
        print('Publishing message to the topic',topic)
        client.publish(topic,data)
        time.sleep(1)
        client.loop_stop()
    except socket.gaierror as e:
        print("Error: Unable to connect to Broker Service")
        print(e)
        

if __name__ == '__main__':

    # Parse command line arguements
    parser = argparse.ArgumentParser()
    parser.add_argument('-d','--directory',action='store',default='/home/mark/log_files/',\
                        help='Provide the directory to store the results locally',\
                        dest='directory')
    parser.add_argument('-b','--broker',action='store',default='192.168.0.100', \
                        help='Provide the MQTT Broker Name',\
                        dest='broker')
    parser.add_argument('-t','--topic',action='store',default='machines/test',\
                        help='Provide the topic to publish to',\
                        dest='topic')
    parser.add_argument('-c','--client',action='store',default='machine-test',\
                        help='Provide a name for the mqtt client',\
                        dest='client')
    #extact results
    results=parser.parse_args()

    log_dir = results.directory
    broker_name = results.broker
    topic = results.topic


    #unit counter variable
    unit_count = 0

    # Init the input trigger
    door_switch = Button(2)
    braze_feeder = Button(4)
    flame_sensor = Button(27)

    if __name__ == '__main__':
        while(True):
            print('creating new instance')
            client = mqtt.Client(results.client)
            door_switch.when_pressed = log_door
            braze_feeder.when_pressed = log_braze
            print('Connected')
            # Wait for a GPIO input     
            pause()
    
