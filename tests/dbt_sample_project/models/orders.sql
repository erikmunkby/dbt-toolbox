with orders as (
    select id as order_id,
    customer as customer_id,
    ordered_at,
    store_id,
    subtotal,
    tax_paid,
    {{ simple_macro() }} as col,
    order_total
    from {{ ref("raw_orders") }}
)
-- my_comment, xxx
select * from orders