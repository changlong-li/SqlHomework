select Id, OrderDate, PrevOrderDate, round(julianday(OrderDate) - julianday(PrevOrderDate), 2) as Difference
from 
	(select Id, OrderDate, lag(OrderDate, 1, OrderDate) over (order by OrderDate asc) as PrevOrderDate
	from 'Order'
	where CustomerId = 'BLONP'
	order by OrderDate asc
	limit 10
	)

