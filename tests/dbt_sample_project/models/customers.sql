with source as (
    select * from {{ ref("raw_customers") }}
),

cleaned as (
    select
        id as customer_id,
        name as full_name
    from source
)

select * from cleaned