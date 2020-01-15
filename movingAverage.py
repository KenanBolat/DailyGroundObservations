import psycopg2
import datetime

start = datetime.datetime.now()
table_name = 'spa_observation'
conn = psycopg2.connect(
    dbname='mgmDb', user='postgres', password='1', port=5433)
curr = conn.cursor()
print('DB Connection Successful !')
curr.execute(f'SELECT DISTINCT(stationid) FROM {table_name};')
allstationids = curr.fetchall()
try:
    curr.execute(
        f'SELECT * INTO {table_name}_smooth FROM {table_name} where stationid = 1000000000')
    print(f'{table_name}_smooth table created.')
    conn.commit()
except:
    print(f'{table_name}_smooth table already exists. Process continues...')

for stationid in allstationids:
    station_start=datetime.datetime.now()
    try:
        curr.execute(f'INSERT INTO {table_name}_smooth\
        SELECT {table_name}.stationid,\
        {table_name}.altitude,\
        {table_name}.m_date,\
        AVG({table_name}.snow_depth)\
        OVER(ORDER BY {table_name}.m_date ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)::INTEGER AS snow_depth,\
        {table_name}.geom \
        FROM {table_name} where stationid = {stationid[0]} order by m_date;')
        conn.commit()
    except:
        print(
            f'There was an error when calculating moving average of station with id: {stationid}')
print(f'{len(allstationids)} station data smoothed in : {datetime.datetime.now()-start } seconds.')
conn.close()
curr.close()
