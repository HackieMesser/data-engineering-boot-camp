/*query public stuff*/
SELECT station_id,name FROM `bigquery-public-data.new_york_citibike.citibike_stations` LIMIT 100;


/*stuck here need to upload these files and then align the csv slugs to my slugs*/
/*need to align table creation to my file structure too*/
CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://nyc-tl-data/trip data/yellow_tripdata_2019-*.csv', 'gs://nyc-tl-data/trip data/yellow_tripdata_2020-*.csv']
);
