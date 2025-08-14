with shopify_orders as (
    select
        "Order ID" as order_id,
        -- The source date may be a string due to the 'DD/MM/YYYY' format. We use TRY_TO_DATE() to safely parse it.
        try_to_date("Order Date", 'DD/MM/YYYY') as order_date,
        "Customer ID" as customer_id,
        "Customer Order Number" as customer_order_number,
        "Market" as market,
        "Discount Code" as discount_code,
        "Order Revenue" as order_revenue,
        try_to_date("First Order Date", 'DD/MM/YYYY') as first_order_date,
        "First Order Discount Code" as first_order_discount_code,
        "Order Tags" as order_tags,
    from {{ source('shopify', 'orders') }}
)

select * from shopify_orders
