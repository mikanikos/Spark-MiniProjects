select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc