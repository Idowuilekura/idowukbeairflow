select
    customer_id,
    count(*) as total_orders,
    count(*) filter (where status = 'completed') as completed_orders,
    sum(case when status = 'completed' then amount else 0 end) as completed_revenue,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date
from {{ ref('stg_orders') }}
group by customer_id
