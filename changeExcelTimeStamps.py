#!/usr/bin/python

#snapshot technique (interpolate or take nearest)
#print out report to screen, to readme (actual time measured total vs given
#create corrected doc

import xlrd
import csv
import fnmatch
import json
from copy import copy
from dateutil import parser
from datetime import datetime, timedelta
import os
import time


def correct_timestamps_learnair():

    print 'WARN: Please ensure your file has no extraneous rows, and that ' + \
    'all data to be processed is in one contiguous measurement, with linear' + \
    ' time steps between each measurement.  Timestamps will be overwritten' + \
    ' with this assumption'

    filename = get_file_recurse_dir_prompt()

    file_dict = []
    #open file, pull out timestamps
    with open(filename) as file:
        reader = csv.DictReader(file)
        [file_dict.append(row) for row in reader]

    start_time, end_time, new_start_time, new_end_time = time_prompt(file_dict)

    #first, correct the timestamps in file_dict object
    print '-----------------------------------------------'
    print 'start time diff: %s' % (new_start_time - start_time)
    print 'end time diff: %s' % (new_end_time - end_time)
    print '-----------------------------------------------'
    print 'duration original: %s' % (end_time - start_time)
    print 'duration actual: %s' % (new_end_time - new_start_time)
    print 'duration diff: %s' % ((new_end_time - new_start_time)-(end_time - start_time))
    print '-----------------------------------------------'

    #correct time to new_start_time + n*(end-start)/len(dict)
    #after this step the times should be the closest to their actual measurement times
    step = (new_end_time - new_start_time) / len(file_dict)

    print ('UPDATING TIMESTAMPS TO RUN FROM ' + \
            new_start_time.strftime("%m/%d/%y %H:%M:%S") + \
            ' TO ' + new_end_time.strftime("%m/%d/%y %H:%M:%S") + \
            ' WITH STEP %s' % step)
    print '-----------------------------------------------'

    for ind, row in enumerate(file_dict):
        for key,val in row.iteritems():
            if any(s in key.lower() for s in ['utc','timestamp']):
                try:
                    #current_time = parser.parse(val)
                    file_dict[ind][key] = new_start_time + ind * step
                except:
                    print 'WARN: cannot parse string to datetime object: %s ' % val
                    print 'WARN: ignoring this row'

    #print_n_entries(file_dict, 15)
    write_dict_to_csv(file_dict, filename[:-4] + '_fixed_timestamps.csv')

    #snap options: snap to nearest min, 30 sec
    #snap options: interpolate or nearest neighbor
    #'rounds' start and end timestamps to snap value
    #snaps over entire duration to desired timestamp

    try:
        round_datetime_to = int(raw_input('Desired Interval in Seconds [30]'))
    except:
        round_datetime_to = 30

    try:
        NEAREST = int(raw_input('Desired Snap, 0=interpolate, 1=nearest [0]'))
    except:
        NEAREST = 0

    interval = timedelta(seconds=round_datetime_to)

    snap_start_time = roundTime(new_start_time-(interval/2), round_datetime_to)
    snap_end_time = roundTime(new_end_time+(interval/2), round_datetime_to)

    print 'snap_start: ' + snap_start_time.strftime("%m/%d/%y %H:%M:%S")
    print 'snap_end: ' + snap_end_time.strftime("%m/%d/%y %H:%M:%S")

    current_time = snap_start_time
    current_row = 0
    snapped_dict = []
    time_key = ''

    #pull out the right key
    for key,val in file_dict[0].iteritems():
        if any(s in key.lower() for s in ['utc','timestamp']):
            time_key = key

    while ( current_time <= snap_end_time ):

        if current_row < len(file_dict):

            #check if current time is greater than current table entry
            #if it is (and we aren't at the end of the dict), iterate
            while (current_time > file_dict[current_row][time_key]):
                current_row = current_row + 1
                if current_row >= len(file_dict):
                    break

        if ( current_row == 0 ):
            #new row is the current row
            new_entry = copy(file_dict[current_row])
            new_entry[time_key] = current_time
            snapped_dict.append(new_entry)

        elif ( current_row >= len(file_dict) ):
            #new row is the last row of file_dict
            new_entry = copy(file_dict[len(file_dict)-1])
            new_entry[time_key] = current_time
            snapped_dict.append(new_entry)

        else: #new row is between two values, use nearest or interpolated
            if NEAREST: #use closest values
                if (current_time - file_dict[current_row - 1][time_key] < \
                        file_dict[current_row][time_key] - current_time):
                    new_entry = copy(file_dict[current_row - 1])
                else:
                    new_entry = copy(file_dict[current_row])

                new_entry[time_key] = current_time
                snapped_dict.append(new_entry)

            else: #interpolate values
                t1 = (current_time - file_dict[current_row - 1][time_key]).total_seconds()
                t2 = (file_dict[current_row][time_key] - current_time).total_seconds()

                new_entry = copy(file_dict[current_row - 1])
                for key, val in new_entry.iteritems():
                    try:
                        new_entry[key] = ( (float(val) * t2) + \
                                (float(file_dict[current_row][key]) * t1) ) / (t1 + t2)
                    except:
                        if key != time_key:
                            print 'failed to average row %s, %s: %s, %s' % \
                                    (current_row, key, val, file_dict[current_row][key])

                new_entry[time_key] = current_time
                snapped_dict.append(new_entry)

        #iterate time by interval
        current_time = current_time + interval

    if NEAREST:
        write_dict_to_csv(snapped_dict, filename[:-4] + '_nearest.csv')
    else:
        write_dict_to_csv(snapped_dict, filename[:-4] + '_interp.csv')


def get_file_recurse_dir_prompt(type='csv', \
        default_folder = '/Users/davidramsay/Documents/thesis/arduinoDataSafe'):
    '''
    asks the user for a search path, recursively finds all files matching
    type in search path, asks users which one they want, returns that filename.
    If no files of type 'type' exit, exits.
    '''

    #get folder of search path
    folder = raw_input('Search Path: [%s] ' % default_folder)
    folder = folder or default_folder

    #get all .csv files in search path
    print '---'
    csv_filenames = []

    for root, dirnames, filenames in os.walk(folder):
        for filename in filenames:
            if filename.lower().endswith('.'+ type):
                csv_filenames.append(os.path.join(root, filename))

    if (len(csv_filenames) == 0):
        print 'no CSV files detected'
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


def time_prompt(file_dict):
    ''' print info about timing from excel sheet, prompt user for new
    times and returns datetime objects'''

    #pull out start time and end time from csv file
    start_time = [value for key,value in file_dict[0].iteritems() \
            if any(s in key.lower() for s in ['utc','timestamp'])]

    end_time = [value for key,value in file_dict[-1].iteritems() \
            if any(s in key.lower() for s in ['utc','timestamp'])]

    if (len(start_time) > 1 or len(end_time) > 1):
        raise TypeError('detected more than one timestamp column in csv')
    elif (len(start_time) == 0 or len(end_time) == 0):
        raise TypeError('unable to detect timestamp column in csv')

    print 'start time: %s \nend time: %s' % (start_time[0], end_time[0])
    print '---'

    #convert to datetime object
    try:
        start_time = parser.parse(start_time[0])
        end_time = parser.parse(end_time[0])
    except:
        raise ValueError('cannot parse string to datetime object')

    #get user input
    new_start_time = raw_input('Provide actual start time of measurement: ')
    new_end_time = raw_input('Provide actual end time of measurement: ')

    #convert to datetime object
    try:
        new_start_time = parser.parse(new_start_time)
        new_end_time = parser.parse(new_end_time)
    except:
        raise ValueError('cannot parse string to datetime object')

    return start_time, end_time, new_start_time, new_end_time


def roundTime(dt=None, roundTo=60):
    """Round a datetime object to any time laps in seconds
    dt : datetime.datetime object, default now.
    roundTo : Closest number of seconds to round to, default 1 minute.
    Author: Thierry Husson 2012 - Use it as you want but don't blame me.
    """
    if dt == None : dt = datetime.datetime.now()
    seconds = (dt - dt.min).seconds
    # // is a floor division, not a comment on
    # following line:
    rounding = (seconds+roundTo/2) // roundTo * roundTo
    return dt + timedelta(0,rounding-seconds,-dt.microsecond)


def print_n_entries(dict, n):
    for i in range(n):
        print_string = ''
        for key,val in dict[i].iteritems():
            if any(s in key.lower() for s in ['utc','timestamp']):
                try:
                    print_string = print_string + ' ' + key + ': ' + val.strftime("%m/%d/%y %H:%M:%S") + ','
                except:
                    print_string = print_string + ' ' + key + ': ' + parser.parse(val).strftime("%m/%d/%y %H:%M:%S") + ','

            else:
                print_string = print_string + ' ' + key + ': ' + str(val) + ','
        print print_string


def write_dict_to_csv(dict, filename):
    print 'writing to ' + filename
    try:
        with open(filename,'w+') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=dict[0].keys())
            writer.writeheader()
            for data in dict:
                writer.writerow(data)
    except IOError as (errno, strerror):
        print("I/O error({0}): {1}".format(errno, strerror))


if __name__=="__main__":
    correct_timestamps_learnair()
