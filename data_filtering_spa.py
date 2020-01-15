import psycopg2
import statistics
import random
import datetime

conn = psycopg2.connect(
    "dbname = MGM user=postgres password = <password> host = 192.168.0.221 port = 5432"
)
print('Database Connection Successfull...')
curr = conn.cursor()
start = datetime.datetime.now()
station_sql = 'SELECT DISTINCT(stationid) from spa_observation'
#TODO Add updating mechanism
"""
SELECT * INTO spa_obs_filter_Backup_191103  FROM spa_obs_filter;
insert into spa_obs_filter(stationid, altitude, m_date, snow_depth, geom) select stationid, altitude, m_date, snow_depth, geom from spa_observation where m_date > '2019-09-25';

"""
# create_table_sql = 'SELECT * INTO spa_obs_filter FROM spa_observation'
curr.execute(station_sql)
stations = curr.fetchall()

# try:
#     curr.execute(create_table_sql)
# except BaseException as be:
#     print(be.pgcode)
#
conn.commit()


def filtering(array, size):
    target_index = int(size / 2)
    fromIn = 0
    toIn = size
    new_array = []
    for i in range(0, len(array) - 1):
        if i < (size - 10) or i > len(array) - 10:
            new_array.extend([array[i]])
        else:
            total = []
            total.extend(array[fromIn:target_index])
            total.extend(array[target_index + 1:toIn])
            mean = sum(total) / (size - 1)
            if array[i] <= mean:
                if mean - array[i] > 50:
                    new_array.extend([999999])
                else:
                    new_array.extend([array[i]])
            elif array[i] >= mean:
                if array[i] - mean < 50:
                    new_array.extend([array[i]])
                else:
                    new_array.extend([999999])
            else:
                new_array.extend([999999])
            fromIn += 1
            toIn += 1
            target_index += 1
    new_array.extend(array[-1:])
    return new_array


for station in stations:
    station_start = datetime.datetime.now()
    curr.execute(
        f'SELECT snow_depth, m_date, stationid FROM spa_observation WHERE stationid = {station[0]} and m_date > \'2019-02-25\' and snow_depth is not NULL and status = 0 ORDER BY m_date ASC')
    temp_observations = curr.fetchall()
    sd = [temp_observations[0] for temp_observations in temp_observations[:]]
    dates = [temp_observations[1]
             for temp_observations in temp_observations[:]]
    ids = [temp_observations[2] for temp_observations in temp_observations[:]]
    filtered_values = filtering(sd, 19)
    rows = []
    for i in range(0, len(filtered_values)):
        rows.append({
            'stationid': ids[i - 1], 'm_date': dates[i - 1], 'snow_depth': filtered_values[i - 1]
        })
    rows = tuple([row for row in rows if row['snow_depth'] == 999999])
    curr.executemany(
        'UPDATE spa_obs_filter SET snow_depth = %(snow_depth)s , status=1 WHERE stationid = %(stationid)s and m_date = %(m_date)s', rows)
    conn.commit()
    print('Station ', station[0],
          f'completed in --> {datetime.datetime.now()-station_start}')

curr.close()
conn.close()
print('Process Exhausted in : ', datetime.datetime.now() - start)
