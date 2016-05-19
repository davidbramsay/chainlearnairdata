#!/usr/bin/python

'''
take json data from learnAirV1 Sensor (mounted on EPA Sensor) in CSV format,
turn into json, and push to Chain.

generalize to EPA data in xls format (push to Site->Device->Sensor)

generalize to API data (find a device object, look at timestamp of data and location,
call api, push api data.  Find a site object, do the same thing)

'''

import itertools
import zmq
import json
import xlrd
import csv
from datetime import datetime
from tzlocal import get_localzone
from dateutil import parser
import pandas as pd
import os


#pull file (XLSX from EPA, CSV from arduino, CSV from SmartCitizen)
#form it into correct dictionary form

#separate scripts that will call changeTimeStamps (and walk through pre-process)
#and then ask to pull the file, form the data, and put it into the correct place
#(one of a couple of predetermined paths to push data to)


def get_file_recurse_dir_prompt(types=['csv','xlsx'], \
        default_folder = '/Users/davidramsay/Documents/thesis/arduinoDataSafe'):
    '''
    asks the user for a search path, recursively finds all files matching
    type in search path, asks users which one they want, returns that filename.
    If no files of type 'type' exit, exits.
    '''

    types = [type.lower() for type in types]

    #get folder of search path
    folder = raw_input('Search Path: [%s] ' % default_folder)
    folder = folder or default_folder

    #get all .csv files in search path
    print '---'
    csv_filenames = []

    for root, dirnames, filenames in os.walk(folder):
        for filename in filenames:
            for type in types:
                if filename.lower().endswith('.'+ type):
                    csv_filenames.append(os.path.join(root, filename))

    if (len(csv_filenames) == 0):
        print 'no files detected'
        exit()

    for i, fn in enumerate(csv_filenames):
        print ('[%d] ' % i + fn)
    print '---'

    #get file to edit
    while 1:
        default_file_index = 0
        file_index = raw_input('File Choice: [0] ')
        try:
            file_index = int(file_index)
        except:
            file_index = default_file_index

        try:
            file = csv_filenames[file_index]
            print '-- SELECTED ' + file
            return file
        except:
            print '- invalid option, try again -'


def pull_file_values(file_path):

    if file_path.lower().endswith('.csv'):
        return pull_csv_values(file_path)
    elif file_path.lower().endswith('.xlsx'):
        return pull_xlsx_values(file_path)
    else:
        raise 'this is not a known filetype'


def pull_csv_values(file_path):

    file_dict = {}

    #open and read file
    df = pd.read_csv(file_path)
    keys = df.keys()

    #get the key for timestamps, and all other keys
    timekey = [key for key in keys if any(s in key.lower() for s in ['utc', 'timestamp'])][0]
    keys = [key for key in keys if not any(s in key.lower() for s in ['utc', 'timestamp'])]

    #add local system timezone to timestamps, and format properly for chain
    timestamps = [parser.parse(time).replace(tzinfo=get_localzone()) \
            for time in df[timekey]]
    timestamps = [datetime.strftime(time, '%Y-%m-%d %H:%M:%S.%f%z') \
            for time in timestamps]

    #for each column, make an array of dicts with 'value' and 'timestamps'
    #the key for each array is the key for the column stripped of leading and
    #trailing whitespace.  Values are assumed to be ints or floats.
    for key in keys:
        new_key = key.strip()
        file_dict[new_key] = []
        for time, val in itertools.izip(timestamps,df[key]):
            try:
                file_dict[new_key].append({'value':int(val),'timestamp':time})
            except:
                try:
                    file_dict[new_key].append({'value':float(val),'timestamp':time})
                except:
                    print 'could not add value : %s' % val

    return file_dict


def pull_xlsx_values(file_path):
    #step 1. pull in XLSX data
    with open(tempPath) as file:
        reader = csv.DictReader(file)

        out = json.dumps( [ row for row in reader ], ensure_ascii=False, encoding="utf-8")

    print out


if __name__ == '__main__':

    excelMainPath ='/Users/davidramsay/Documents/thesis/arduinoDataSafe'
    #locate excel sheet folders
    print 'search path: %s' % excelMainPath
    print 'make sure to select a file that has been preprocessed so timestamps are \
            correct and the data is well-formed'

    file_to_upload = get_file_recurse_dir_prompt()
    values_to_upload = pull_file_values(file_to_upload)
    #go through each key in values to upload and figure out where it should go
    #(i.e. sensor type), convert each key to json before uploading


#step 2. format to JSON


#step 3. crawl and find device

#step 4. locate all sensor URIs

#step 5. match sensors with json data, push sensordata.  if no match, create sensor.




