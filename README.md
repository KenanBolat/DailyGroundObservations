"# DailyGroundObservations" 
---
## TODO 
### Download the neccassary txt files from the FTP 
### Run ``` groundObs2Pg.py``` script from the last available date and the most current date downloaded from the server 
### Run following sql sentences 
- For AWOS <br>
```insert into aws_obs_filter(stationid, altitude, m_date, snow_depth, geom) select stationid, altitude, m_date, snow_depth, geom from aws_observation where m_date > '2019-11-01';```
- For SPA <br>
   ```insert into spa_obs_filter(stationid, altitude, m_date, snow_depth, geom) select stationid, altitude, m_date, snow_depth, geom from spa_observation where m_date > '2019-11-01';```
 ### Run ```data_filtering.py```
 ### Run ```data_filtering_spa.py```
## To export observations as txt for validation software run the following sql commands for the given database 
```SQL
copy (select stationid, st_x(geom) as x , st_y(geom) as y , 0 as alt, extract(year from m_date) as year, extract(month from m_date) as month, extract(day from m_date) as day, extract(hour from m_date) as hour, extract(minute from m_date) as min, coalesce(nullif(snow_depth, NULL), 9999)  from aws_observation where m_date >= '2020-10-01' and m_date<= '2021-10-01') TO '/var/log/postgresql/2020_2021_aws_observation_full.txt' DELIMITER E'\t';
copy (select stationid, st_x(geom) as x , st_y(geom) as y , 0 as alt, extract(year from m_date) as year, extract(month from m_date) as month, extract(day from m_date) as day, extract(hour from m_date) as hour, extract(minute from m_date) as min, coalesce(nullif(snow_depth, NULL), 9999)  from spa_observation where m_date >= '2020-10-01' and m_date<= '2021-10-01') TO '/var/log/postgresql/2020_2021_spa_observation_full.txt' DELIMITER E'\t'
copy (select stationid, st_x(geom) as x , st_y(geom) as y , 0 as alt, extract(year from m_date) as year, extract(month from m_date) as month, extract(day from m_date) as day, extract(hour from m_date) as hour, extract(minute from m_date) as min, coalesce(nullif(snow_depth, NULL), 9999)  from syn_observation where m_date >= '2020-10-01' and m_date<= '2021-10-01') TO '/var/log/postgresql/2020_2021_syn_observation_full.txt' DELIMITER E'\t'
```

### Run following sql sentences 

 ###Add geometric index and table index increased the code's performance tremendously
```SQL

insert into aws_obs_filter select stationid, altitude , m_date , snow_depth , geom from aws_observation ao where m_date > '2020-01-13';
insert into spa_obs_filter select stationid, altitude , m_date , snow_depth , geom from spa_observation ao where m_date > '2020-01-13';
create index aws_obs_filter_geom_idx on aws_obs_filter using gist(geom);
create index spa_obs_filter_geom_idx on spa_obs_filter using gist(geom);

REFRESH MATERIALIZED VIEW daily_aws_3857; 
REFRESH MATERIALIZED VIEW daily_spa_3857;
REFRESH MATERIALIZED VIEW daily_syn_3857;
REFRESH MATERIALIZED VIEW daily_sck_3857;
REFRESH MATERIALIZED VIEW daily_aws_filtered;
REFRESH MATERIALIZED VIEW daily_spa_filtered;
REFRESH MATERIALIZED VIEW daily_syn_filtered;
refresh materialized view stations_aws;
refresh materialized view stations_syn;
refresh materialized view stations_sck;
refresh materialized view stations_sck_simple;
refresh materialized view stations_spa;
```

 
