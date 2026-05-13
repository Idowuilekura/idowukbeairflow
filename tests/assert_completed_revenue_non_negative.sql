select *
from {{ ref('customer_order_summary') }}
where completed_revenue < 0
