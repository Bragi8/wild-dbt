-- This model creates the final fact table for customer acquisition analysis.
-- It joins new customer orders with marketing costs to attribute acquisition channels.

with first_orders as (
    -- Use our intermediate model which is already filtered to new customers.
    select * from {{ ref('int_customer_first_orders') }}
),

influencer_affiliate_costs as (
    -- Reference the marketing source table directly and perform cleaning here.
    select
        "Channel" as channel,
        "Discount_Code" as discount_code,
        "Market" as market,
        try_to_date("Live_Date", 'DD/MM/YYYY') as live_date,
        "Cost" as cost
    from {{ source('marketing', 'int_marketing_inf_aff_costs') }}
),

paid_social_costs as (
    -- Reference the marketing source table directly and perform cleaning here.
    select
        "Channel" as channel,
        null as discount_code, -- Paid social does not use discount codes.
        "Market" as market,
        try_to_date("Date", 'DD/MM/YYYY') as cost_date,
        "Cost" as cost
    from {{ source('marketing', 'int_marketing_paid_social_costs') }}
),

-- This CTE unions the two cost sources into one unified view of marketing spend.
unioned_costs as (
    select * from influencer_affiliate_costs
    union all
    select * from paid_social_costs

),

-- Final CTE to join acquisitions (orders) with costs.
final as (
    select
        -- Key order details from the first_orders table.
        o.order_id,
        o.order_created_at,
        o.customer_id,
        o.market,
        o.order_revenue,

        -- Attribution logic:
        coalesce(uc.channel, 'Organic') as acquisition_channel,
        coalesce(uc.cost, 0) as marketing_spend

    from first_orders o
    left join unioned_costs uc
        -- We join on the discount code to attribute influencer and affiliate orders.
        on o.discount_code = uc.discount_code
        and o.market = uc.market
)

select * from final
