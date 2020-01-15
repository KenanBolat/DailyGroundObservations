
import csv
import os
import glob
import datetime

path = r'I:\Dev\SPA'
fname = 'spa_karsu.txt'




with open(fname, 'r') as csvfile:
    reader = csv.reader(csvfile, delimiter=";")

    for en, row in enumerate(reader):
        date = datetime.datetime.strptime(":".join(row[1:6]), "%Y:%m:%d:%H:%M")
        stationid = row[0]
        m_date = date
        snow_depth = None
        geom = None
        snow_percentage_A1 = None
        water_percentage_A1 = None
        snow_density_A1 = None
        SWE_A1 = None
        snow_percentage_A2 = None
        water_percentage_A2 = None
        snow_density_A2 = None
        SWE_A2 = None
        status = None
        print(date)
        if en > 10:

            break

connection = mysql.connector.connect(user="knn",
                                     password="kalman",
                                     host="192.168.0.197",
                                     database="MGM")

cur = connection.cursor()

# cur.execute("INSERT INTO people (name, age) VALUES ('Bob', 25);")
cur.execute("select * from spa_observations_with_all_measurements where StationID = 17777;")
for i in cur.fetchall():
    print(i)
connection.commit()

cur.close()
connection.close()
