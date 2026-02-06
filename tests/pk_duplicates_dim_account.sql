select account_sk
from {{ ref('dim_account') }}
group by account_sk
having count(*) > 1
