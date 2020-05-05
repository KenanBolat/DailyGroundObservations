select * from stations_aws sa ;

select * from aws_obs_filter aof ;

select max(m_date ) from aws_obs_filter;

select * from aws_obs_filter aof limit 1 ;

select * from aws_observation ao limit 1 ;

insert into aws_obs_filter
select
stationid,
max(altitude) as altitude ,
m_date::date as m_date ,
avg(snow_depth ) ,
geom
from aws_observation ao
where m_date::date = '2020-01-13'
group by m_date,stationid , geom;



ALTER TABLE aws_obs_filter SET (FILLFACTOR = 70);
VACUUM FULL aws_obs_filter;
REINDEX TABLE aws_obs_filter;


select * from daily_aws_filtered daf where m_date > '2020-01-13';

select * from aws_obs_filter where m_date::date = '2020-02-14' and stationid = 18706;

select * from aws_obs_filter aof where m_date::date = '2020-01-14';
select * from aws_observation ao where snow_depth > 999;

-- delete from aws_obs_filter aof where m_date::date >= '2020-01-13';


select max(m_date ) from spa_observation so ;


select * from spa_observation so ;


select * from spa_obs_filter sof ;

select max(m_date ) from spa_observation sof ;
--insert into spa_obs_filter
select * from spa_observation sof where m_date > '2020-01-13' and snow_depth is null;
select max(m_date ) from spa_obs_filter sof ;


select max(m_date) from spa_obs_filter sof ;
refresh materialized view daily_spa_filtered;
refresh materialized view stations_aws ;
refresh materialized view stations_spa ;
refresh materialized view stations_sck ;
refresh materialized view stations_sck_simple ;



select m_date, count(stationid ) from daily_spa_filtered dsf where m_date  > '2020-01-13' group by m_date ;
select * from daily_spa_filtered dsf ;

select row_number() over (order by stationid ), stationid, st_x(geom ) as x, st_Y(geom) as y, max(altitude ) from spa_observation so group by stationid, geom


SELECT * FROM spa_obs_filter;
SELECT * FROM aws_obs_filter;
select * from sck_observation so ;
select * from aws_observation ao ;
select * from spa_observation ao ;


create index aws_obs_filter_geom_idx on aws_obs_filter using gist(geom);
create index spa_obs_filter_geom_idx on spa_obs_filter using gist(geom);

select max(m_date ) from aws_obs_filter;



insert into aws_obs_filter
select
stationid,
max(altitude) as altitude ,
m_date::date as m_date ,
avg(snow_depth ) ,
geom
from aws_observation ao
where m_date::date > '2020-01-13'
group by m_date,stationid , geom;


select max(m_date ) from aws_observation ao ;


select * from aws_obs_filter aof limit 1 ;
select * from aws_obs_filter aof where m_date > '2020-01-13';
delete from aws_obs_filter aof where m_date > '2020-01-13';
select count(*) from aws_obs_filter aof where m_date > '2020-01-13';
insert into aws_obs_filter select stationid, altitude , m_date , snow_depth , geom from aws_observation ao where m_date > '2020-01-13';




select ao.m_date, ao.snow_depth, aof.snow_depth  from aws_obs_filter aof left join aws_observation ao on aof.stationid =ao.stationid where aof.

select * from aws_obs_filter where m_date > '2020-01-13' and stationid = 18706 order by 3 asc;
select * from aws_observation ao where m_date > '2020-01-13' and stationid = 18706 order by 3 asc;




refresh materialized view stations_aws;
refresh materialized view stations_syn;
refresh materialized view stations_sck;
refresh materialized view stations_sck_simple;
refresh materialized view stations_spa;
REFRESH MATERIALIZED VIEW daily_aws_filtered;
REFRESH MATERIALIZED VIEW daily_spa_filtered;
REFRESH MATERIALIZED VIEW daily_syn_filtered;
REFRESH MATERIALIZED VIEW daily_aws_3857;
REFRESH MATERIALIZED VIEW daily_spa_3857;
REFRESH MATERIALIZED VIEW daily_syn_3857;
REFRESH MATERIALIZED VIEW daily_sck_3857;
