
"""
Purpose: MGM Ground Observation Read Routines from daily multiple csv files
Author : Kenan BOLAT
"""

import datetime
import csv
import os
import datahandle
import numpy as np
import mysql.connector
start = datetime.datetime.now()
print start

cnx = mysql.connector.connect(
    host="192.168.0.199",
    user="knn",
    passwd="kalman",
    database="Hidrosaf"
)
cursor = cnx.cursor()

add_data = "INSERT INTO sck_observation (StationID, Latitude, Longitude, Altitude, Year, Month, Day, Hour, Temperature) " \
           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

LOG = []

c = np.array([])

# Define Path
data_path = r"I:\temp\DailyGroundObservations\DailyGroundObservations\sck"
merged_file = []
b = []
# Trace each file and replace "|"  with tab and "NULL" with 9999
for r, d, f in os.walk(data_path):
    print d
    for i in f:
        filename = os.path.join(r, i)

        file_tags = datahandle.Handle.get_file_tags(filename)
        print file_tags
        datahandle.Handle.write_data(file_tags[1])
        with open(filename, 'rb') as csvfile:
            spamreader = csv.reader(csvfile)
            for row in spamreader:
                with open(file_tags[1], 'a') as csvfile:
                    if row[0].find("NULL") != -1:
                        LOG.append(filename)
                    a = row[0].replace("NULL", "9999")
                    if a.find("||") != -1 or a.find("CCA") or a.find("SCA"):
                        LOG.append(filename)
                    a = row[0].replace("NULL", "9999")
                    a = a.replace("CAA", "")
                    a = a.replace("CAA", "")
                    a = a.replace("CAB", "")
                    a = a.replace("CCB", "")
                    a = a.replace("CCC", "")
                    a = a.replace("CCD", "")
                    a = a.replace("CCE", "")
                    a = a.replace("CCF", "")
                    a = a.replace("RAA", "")
                    a = a.replace("RRA", "")
                    a = a.replace("RRN", "")
                    a = a.replace("VVU", "")
                    a = a.replace("||", "|")
                    data = ([float(row) for row in a.split("|")])
                    try:
                        cursor.execute(add_data, tuple(data))
                        cnx.commit()
                    except BaseException as be:
                        print be.message
                        spamwriter = csv.writer(csvfile)
                        spamwriter.writerow([filename])


end = datetime.datetime.now()
print end - start


print "=" * 100
