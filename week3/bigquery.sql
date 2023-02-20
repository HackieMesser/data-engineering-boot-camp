/*query public stuff*/
SELECT station_id,name FROM `bigquery-public-data.new_york_citibike.citibike_stations` LIMIT 100;


/*stuck here need to upload these files and then align the csv slugs to my slugs*/
/*need to align table creation to my file structure too*/
CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://nyc-tl-data/trip data/yellow_tripdata_2019-*.csv', 'gs://nyc-tl-data/trip data/yellow_tripdata_2020-*.csv']
);

select distinct(VendorID) from `silicon-airlock-376018.trips_data_all.yellowtrips` where date(tpep_pickup_datetime) between '2019-06-01' and '2019-06-30';
select distinct(VendorID) from `silicon-airlock-376018.trips_data_all.yellowtrips_partitioned` where date(tpep_pickup_datetime) between '2019-06-01' and '2019-06-30';

select table_name, partition_id, total_rows
from `silicon-airlock-376018.trips_data_all.INFORMATION_SCHEMA.PARTITIONS`
where table_name = 'yellowtrips_partitioned'
order by total_rows desc;
