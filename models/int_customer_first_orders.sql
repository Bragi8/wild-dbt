with orders as (
    select * from {{ ref('stg_shopify__orders') }}
)

select *
from orders
where customer_order_number = 1
