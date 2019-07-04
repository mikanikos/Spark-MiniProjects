select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from
	shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year