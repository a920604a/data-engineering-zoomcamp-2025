with
    clean_fact_trips as (
        select
            service_type,
            extract(year from pickup_datetime) as year,
            extract(month from pickup_datetime) as month,
            fare_amount,
            trip_distance,
            payment_type_description
        from {{ ref("fact_trips") }}
        where
            fare_amount > 0
            and trip_distance > 0
            and lower(payment_type_description) in ('cash', 'credit card')
    ),

    fare_amt_perc as (
        select
            service_type,
            year,
            month,
            percentile_cont(fare_amount, 0.97) over (
                partition by service_type, year, month
            ) as p97,
            percentile_cont(fare_amount, 0.95) over (
                partition by service_type, year, month
            ) as p95,
            percentile_cont(fare_amount, 0.90) over (
                partition by service_type, year, month
            ) as p90
        from clean_fact_trips
    )

select *
from fare_amt_perc
