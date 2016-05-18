#!/usr/bin/python

'''
take json data from learnAirV1 Sensor (mounted on EPA Sensor) in CSV format,
turn into json, and push to Chain.

generalize to EPA data in xls format (push to Site->Device->Sensor)

generalize to API data (find a device object, look at timestamp of data and location,
call api, push api data.  Find a site object, do the same thing)

'''
import zmq
import json
import xlrd
import csv
import os

#uri = "http://learnair.media.mit.edu:8000/devices/10"


#function that looks for csv sheets and asks user for metadata about them
#and whether to post them

#main folder
excelMainPath ='/Users/davidramsay/Documents/thesis/arduinoDataSafe'
#locate excel sheet folders
excelDateFolders = [x for x in os.listdir(excelMainPath) if 'takenDown' in x]
#enter folder and get excelData


##function that takes excel path to folder, device name, and sensor
#meta-params and puts it up to chain

tempPath ='/Users/davidramsay/Documents/thesis/arduinoDataSafe/takenDown042516/boxArduino042516.CSV'
deviceName = 'test004'

#timestamp    alphaS1_work    alphaS1_aux     alphaS2_work    alphaS2_aux     alphaS3_work    alphaS3_aux     alphaTemp   sharpDust   pressureWind
#4/15/16 06:14   973 984 975 6   975 177 62  817 0

#step 1. pull in XLSX data
with open(tempPath) as file:
    reader = csv.DictReader(file)

    out = json.dumps( [ row for row in reader ], ensure_ascii=False, encoding="utf-8")

print out

#step 2. format to JSON

#step 3. crawl and find device

#step 4. locate all sensor URIs

#step 5. match sensors with json data, push sensordata.  if no match, create sensor.




