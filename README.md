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
### Run following sql sentences 
 
```SQL
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

 