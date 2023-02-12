-- SETUP
create or replace external table
dezdataset.ext_tab_fhv_2019
options(
format='csv',
uris = ['gcs://week2_dez_gcs_delete_soon/data/dataweek3DEZ/fhv_tripdata_2019-01.csv', 'gcs://week2_dez_gcs_delete_soon/data/dataweek3DEZ/fhv_tripdata_2019-02.csv', 'gcs://week2_dez_gcs_delete_soon/data/dataweek3DEZ/fhv_tripdata_2019-03.csv', 'gcs://week2_dez_gcs_delete_soon/data/dataweek3DEZ/fhv_tripdata_2019-04.csv', 'gcs://week2_dez_gcs_delete_soon/data/dataweek3DEZ/fhv_tripdata_2019-05.csv', 'gcs://week2_dez_gcs_delete_soon/data/dataweek3DEZ/fhv_tripdata_2019-06.csv', 'gcs://week2_dez_gcs_delete_soon/data/dataweek3DEZ/fhv_tripdata_2019-07.csv', 'gcs://week2_dez_gcs_delete_soon/data/dataweek3DEZ/fhv_tripdata_2019-08.csv', 'gcs://week2_dez_gcs_delete_soon/data/dataweek3DEZ/fhv_tripdata_2019-09.csv', 'gcs://week2_dez_gcs_delete_soon/data/dataweek3DEZ/fhv_tripdata_2019-10.csv', 'gcs://week2_dez_gcs_delete_soon/data/dataweek3DEZ/fhv_tripdata_2019-11.csv', 'gcs://week2_dez_gcs_delete_soon/data/dataweek3DEZ/fhv_tripdata_2019-12.csv']
)

create or replace table
  `dezdataset.tab_fhv_2019`
as
  select * from `dezdataset.ext_tab_fhv_2019`

--Q1
select
  count(*)
from
  `dezdataset.ext_tab_fhv_2019`

--Q2
select
  count(distinct(affiliated_base_number))
from
  `dezdataset.ext_tab_fhv_2019`

--Q3
select
  count(*)
from
  `dezdataset.tab_fhv_2019`
where
  PUlocationID is null
and
  DOlocationID is null

--Q4
create or replace table
  `dezdataset.tab_fhv_2019_clustered_on_pickupdate_cluster_on_affiliatedbasenum`
cluster by
  date(pickup_datetime),
  string(affiliated_base_number)
as select * from `dezdataset.tab_fhv_2019`

--Q5
select
  distinct(affiliated_base_number)
from
  `dezdataset.tab_fhv_2019_partition_by_pickupdate_cluster_on_affiliatedbasenum`
where
  date(pickup_datetime) between date('2019-03-01') and date('2019-03-31')


--Q6

--Q7

