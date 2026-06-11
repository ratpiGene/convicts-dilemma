{% test is_rate(model, column_name) %}
-- Custom generic test: the column must be a valid rate in [0, 1]
-- (nulls allowed — e.g. forgiveness_rate when a strategy was never
-- betrayed; pair with not_null where nulls are forbidden).

select *
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < 0 or {{ column_name }} > 1)

{% endtest %}
