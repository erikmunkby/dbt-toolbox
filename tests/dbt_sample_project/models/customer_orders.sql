{{
    config(
        materialized="ephemeral",
    )
}}
with customers as (
    select customer_id, full_name from {{ ref("customers") }}
),

orders as (
    select order_id, ordered_at, store_id, subtotal, tax_paid, order_total, customer_id from {{ ref("orders") }}
),

customer_orders as (
    select
        c.customer_id,
        c.full_name,
        o.order_id,
        o.ordered_at,
        o.store_id,
        o.subtotal,
        o.tax_paid,
        o.order_total,
        {{ simple_macro() }} as macro_column
    from customers c
    left join orders o
        on c.customer_id = o.customer_id
)

select * from customer_orders 