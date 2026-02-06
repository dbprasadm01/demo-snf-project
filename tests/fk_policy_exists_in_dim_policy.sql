select distinct f.policy_id
from {{ ref('fct_claim_payments') }} f
left join {{ ref('dim_policy') }} d on f.policy_id = d.policy_id
where d.policy_id is null
