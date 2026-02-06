select payment_id
from {{ ref('fct_claim_payments') }}
group by payment_id
having count(*) > 1
