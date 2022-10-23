with Expenditures as
	(
	select ifnull(Customer.CompanyName, 'MISSING_NAME') as CompanyName, 'Order'.CustomerId, round(sum(OrderDetail.UnitPrice * OrderDetail.Quantity), 2) as TotalExpenditures
	from 'Order'
	inner join OrderDetail on OrderDetail.OrderId = 'Order'.Id
	left join Customer on Customer.Id = 'Order'.CustomerId
	group by 'Order'.CustomerId
	)
,
Quartiles as
	(
	select *, ntile(4) over (order by TotalExpenditures asc) as ExpenditureQuartile
	from Expenditures
	)
	
select CompanyName, CustomerId, TotalExpenditures
from Quartiles
where ExpenditureQuartile = 1
order by TotalExpenditures asc
;
