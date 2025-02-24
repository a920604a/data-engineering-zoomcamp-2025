with quarterly_revenue as (
    select 
        extract(year from pickup_datetime) as year,
        extract(quarter from pickup_datetime) as quarter,
        concat(extract(year from pickup_datetime), '/Q', extract(quarter from pickup_datetime)) as year_quarter,
        taxi_type,  -- Assuming we have a column to distinguish between Green and Yellow taxis
        sum(total_amount) as quarterly_revenue
    from `keen-dolphin-450409-m8.dbt_ychen.fct_taxi_trips`
    group by 1, 2, 3, 4
),

yoy_growth as (
    select 
        curr.year, 
        curr.quarter, 
        curr.year_quarter,
        curr.taxi_type,
        curr.quarterly_revenue,
        prev.quarterly_revenue as prev_year_revenue,
        round(
            (curr.quarterly_revenue - prev.quarterly_revenue) / nullif(prev.quarterly_revenue, 0) * 100, 
            2
        ) as yoy_revenue_growth
    from quarterly_revenue curr
    left join quarterly_revenue prev 
        on curr.taxi_type = prev.taxi_type
        and curr.quarter = prev.quarter
        and curr.year = prev.year + 1  -- Join with the previous year's same quarter
)

select * from yoy_growth
order by year desc, quarter;
