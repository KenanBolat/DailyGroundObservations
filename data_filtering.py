
import psycopg2
import statistics
import random
import datetime

conn = psycopg2.connect(
    "dbname = MGM user=postgres password = kalman host =localhost port = 5432"
)

print('Database Connection Successfull...')
curr = conn.cursor()
start = datetime.datetime.now()
# station_sql = 'SELECT DISTINCT(stationid) from aws_observation'
station_sql = 'SELECT DISTINCT(stationid) from aws_observation'
# create_table_sql = 'SELECT * INTO aws_obs_filter FROM aws_observation'
# insert into spa_obs_filter(stationid, altitude, m_date, snow_depth, geom) select stationid, altitude, m_date, snow_depth, geom from spa_observation where m_date > '2019-09-25';
curr.execute(station_sql)
stations = curr.fetchall()

# try:
#     curr.execute(create_table_sql)
# except BaseException as be:
#     print(be.pgcode)

conn.commit()


def filtering(array, size):
    target_index = int(size / 2)
    fromIn = 0
    toIn = size
    new_array = []
    for i in range(len(array)):
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
    # new_array.extend(array[-1:])
    return new_array


for station in stations:
    station_start = datetime.datetime.now()
    print(station)
    curr.execute(
        f'SELECT snow_depth, m_date, stationid FROM aws_observation WHERE stationid = {station[0]} and m_date > \'2020-05-31\' ORDER BY m_date ASC')
    temp_observations = curr.fetchall()
    sd = [temp_observations[0] for temp_observations in temp_observations[:]]
    dates = [temp_observations[1]
             for temp_observations in temp_observations[:]]
    ids = [temp_observations[2] for temp_observations in temp_observations[:]]
    filtered_values = filtering(sd, 20)

    rows = []
    for i in range(0, len(filtered_values)):
        rows.append({
            'stationid': ids[i], 'm_date': dates[i], 'snow_depth': sd[i]
        })

    rows_filtered = []
    for i in range(0, len(filtered_values)):
        rows_filtered.append({
            'stationid': ids[i], 'm_date': dates[i], 'snow_depth': filtered_values[i]
        })
    set_filtered = filtered_values
    set_unfiltered = [r['snow_depth'] for r in rows]
    filtered_index = []
    for en, i in enumerate(set_filtered):
        if not i == set_unfiltered[en]:
            filtered_index.append(en)

    rows_to_be_updated = [rows_filtered[i] for i in filtered_index]
    print(station, "==>", filtered_index.__len__(), rows.__len__())
    curr.executemany(
        'UPDATE aws_obs_filter SET snow_depth = %(snow_depth)s WHERE stationid = %(stationid)s and m_date = %(m_date)s', rows_to_be_updated)
    conn.commit()
    print('Station ', station[0],
          f'completed in --> {datetime.datetime.now()-station_start}')

curr.close()
conn.close()
print('Process Exhausted in : ', datetime.datetime.now() - start)
