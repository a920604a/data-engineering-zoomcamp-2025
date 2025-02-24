select * 
from {{ source('raw_nyc_tripdata', 'ext_green_taxi' ) }}