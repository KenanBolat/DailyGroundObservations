"""

"""
import datetime
import csv
import os
import datahandle
import numpy as np
import psycopg2
import re
from tkinter import filedialog

log = []


class Data_Migration(object):
    def __init__(self):
        self.starttime = datetime.datetime.now()
        self.__process_path = r'C:\Users\user\Documents\GitHub\MGM\veriler\DailyGroundObservations\sck2'
        self.port = 5433
        self.type_ = None
        self.password = '1'
        self.dbname = 'mgmDb'
        self.user = 'postgres'
        self.insertSql = None
        self.conn = psycopg2.connect(
            dbname=self.dbname, user=self.user, password=self.password, port=self.port)
        self.curr = self.conn.cursor()
        print("Program Started : ", self.starttime)

    def process_type(self, type_):
        self.type_ = type_

    @property
    def data_dir(self):
        return self.__process_path

    @data_dir.setter
    def data_dir(self, value):
        self.__process_path = value

    @data_dir.getter
    def data_dir(self):
        return self.__process_path

    @staticmethod
    def reerange_data_TMP(data_):
        data = ([float(row) for row in data_.split("|")][0],  # station_id
                [float(row) for row in data_.split("|")][3],  # Altitude
                [row for row in data_.split("|")][5],  # Month
                [row for row in data_.split("|")][6],  # Day
                [row for row in data_.split("|")][4],  # Year
                [row for row in data_.split("|")][7],  # Hour
                [float(row) for row in data_.split("|")][8],  # Temperature
                [float(row) for row in data_.split("|")][2],  # Longitude
                [float(row) for row in data_.split("|")][1])  # Latitude
        return data

    @staticmethod
    def reerange_data_AWS_or_SPA(data_):
        data = ([float(row) for row in data_.split("|")][0],  # station_id
                [float(row) for row in data_.split("|")][3],  # altitude
                [row for row in data_.split("|")][5],  # Month
                [row for row in data_.split("|")][6],  # Day
                [row for row in data_.split("|")][4],  # Year
                [row for row in data_.split("|")][7],  # Hour
                [row for row in data_.split("|")][8],  # Minute
                [float(row) for row in data_.split("|")][9],  # Snow Height
                [float(row) for row in data_.split("|")][2],  # Longitude
                [float(row) for row in data_.split("|")][1])  # Latitude
        return data

    def reerange_data_SYN(self, data_):
        data = ([float(row) for row in data_.split("|")][0],  # station_id
                [float(row) for row in data_.split("|")][3],  # altitude
                [row for row in data_.split("|")][5],  # Month
                [row for row in data_.split("|")][6],  # Day
                [row for row in data_.split("|")][4],  # Year
                [row for row in data_.split("|")][7],  # Hour
                [float(row) for row in data_.split("|")][8],  # Temperature
                [float(row) for row in data_.split("|")][2],  # Longitude
                [float(row) for row in data_.split("|")][1])  # Latitude
        return data

    def write_to_db(self, data_):
        try:
            self.curr.execute(self.getavailablesql_for_type % data_)
        except BaseException as be:
            log.append(be)
        self.conn.commit()

    @property
    def getavailablesql_for_type(self):
        temp_dict = {
            'TMP': "INSERT INTO sck_observation (stationid, altitude, m_date, temperature, geom) VALUES (%s, %s, TO_TIMESTAMP('%s-%s-%s %s:00:00', 'MM/DD/YYYY HH24:MI:SS'), %s, ST_SetSRID(ST_MakePoint(%s,%s),4326))",
            'AWS': "INSERT INTO aws_observation (stationid, altitude, m_date, snow_height, geom) VALUES (%s, %s, TO_TIMESTAMP('%s-%s-%s %s:%s:00', 'MM/DD/YYYY HH24:MI:SS'), %s, ST_SetSRID(ST_MakePoint(%s,%s),4326))",
            'SYN': "INSERT INTO syn_observation (stationid, altitude, m_date, temperature, geom) VALUES (%s, %s, TO_TIMESTAMP('%s-%s-%s %s:00:00', 'MM/DD/YYYY HH24:MI:SS'), %s, ST_SetSRID(ST_MakePoint(%s,%s),4326))",
            'SPA': "INSERT INTO spa_observation (stationid, altitude, m_date, temperature, geom) VALUES (%s, %s, TO_TIMESTAMP('%s-%s-%s %s:%s:00', 'MM/DD/YYYY HH24:MI:SS'), %s, ST_SetSRID(ST_MakePoint(%s,%s),4326))"
        }
        return temp_dict[self.type_]

    def read_and_write_files(self):
        data_path = self.__process_path
        for r, d, f in os.walk(data_path):
            for i in f:
                file_time = datetime.datetime.now()
                filename = os.path.join(r, i)
                file_tags = datahandle.Handle.get_file_tags(filename)
                a = np.array([0])
                with open(filename, 'r') as csvfile:
                    spamreader = csv.reader(csvfile)
                    for row in spamreader:
                        if re.match(r'^([0-9])*', row[0]).group(0):
                            with open(file_tags[1], 'a') as csvfile:
                                tableAbb = ''
                                if row[0].find("NULL") != -1:
                                    log.append(filename)
                                a = row[0].replace("NULL", "9999")
                                if a.find("||") != -1 or a.find("CCA") or a.find("SCA"):
                                    log.append(filename)
                                a = row[0].replace("NULL", "9999")
                                a = a.replace("CCA", "")
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
                                data = a
                                if "aws" in file_tags[1]:
                                    data = self.reerange_data_AWS_or_SPA(data)
                                    self.type_ = "AWS"
                                elif "spa" in file_tags[1]:
                                    data = self.reerange_data_AWS_or_SPA(data)
                                    self.type_ = "SPA"
                                elif "sinop" in file_tags[1]:
                                    data = self.reerange_data_SYN(data)
                                    self.type_ = "SYN"
                                elif "sck" in file_tags[1]:
                                    data = self.reerange_data_TMP(data)
                                    self.type_ = "TMP"
                                self.write_to_db(data)
                                del a
                        else:
                            print('Error in line: ',row[0] )
                    print(filename, "--->>>  Duration : ",
                          datetime.datetime.now() - file_time)
        print("Total Duration : ", datetime.datetime.now() - self.starttime)


dat = Data_Migration()
dat.read_and_write_files()
