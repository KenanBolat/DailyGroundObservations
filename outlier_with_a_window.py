import numpy as np

import pandas as pd

import numpy as np

import pandas as pd
import pandas.io.sql as sqlio
import psycopg2


def get_all_stations(obs_type):
    conn = psycopg2.connect(
        "host='{}' port={} dbname='{}' user={} password={}".format('192.168.0.197', 5432, 'MGM', 'postgres', 'kalman'))
    sql = "select distinct(stationid) as id  from "+obs_type+"_observation;"
    dat = sqlio.read_sql_query(sql, conn)
    conn.close()
    return dat


def run_for_station(stationid, obs_type):
    conn = psycopg2.connect(
        "host='{}' port={} dbname='{}' user={} password={}".format('192.168.0.197', 5432, 'MGM', 'postgres', 'kalman'))
    sql = "select *  from "+obs_type+"_observation where stationid = "+str(stationid)+" order by m_date desc ;"
    dat = sqlio.read_sql_query(sql, conn)
    conn = None

    n = 100

    # print(dat['snow_depth'])
    # set window size
    window = 10
    std_filt = 50  # I set it at just 1; with real data and larger windows, can be larger

    # create df with rolling stats, upper and lower bounds
    bounds = pd.DataFrame({'mean': dat['snow_depth'].rolling(window).mean()})

    bounds['upper'] = bounds['mean'] + std_filt
    bounds['lower'] = bounds['mean'] - std_filt

    # here, we set an identifier for each window which maps to the original df
    # the first six rows are the first window; then each additional row is a new window
    # bounds['window_id'] = np.append(np.zeros(window), np.arange(1, n - window + 1))

    # then we can assign the original 'b' value back to the bounds df
    bounds['snow_depth'] = dat['snow_depth']

    # and finally, keep only rows where b falls within the desired bounds
    a = bounds.loc[bounds.eval("lower < snow_depth <upper")]
    b = dat.loc[a.index]
    print(len(a), len(b))

    conn = psycopg2.connect(
        "dbname = MGM user=postgres password = kalman host = 192.168.0.197 port = 5432"
    )
    # print('Database Connection Successfull...')
    curr = conn.cursor()

    for row in b.values:
        try:
            query_string = 'INSERT INTO aws_obs_filter_2 VALUES (%(stationid)s,%(altitude)s,  %(m_date)s, %(snow_depth)s)'
            # print(query_string)
            curr.execute(query_string, dict(stationid=str(row[0]), altitude=row[1], m_date=row[2], snow_depth=str(row[3])))
            conn.commit()
        except BaseException as be:
            print(be.message)
            continue
    curr.close()
    conn.close()

import datetime
# for station in get_all_stations().values:
observation_type = 'spa'
for station in get_all_stations(observation_type).values:
    st = datetime.datetime.now()
    run_for_station(station[0],observation_type)
    print(station[0], datetime.datetime.now() - st)
