with source as (
    select id, name from {{ ref("raw_customers") }}
),

cleaned as (
    select
        id as customer_id,
        name as full_name
    from source
)

select customer_id, full_name from cleaned