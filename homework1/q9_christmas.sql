with Info1 as
(
	select Product.Id, ProductName
	from 'Order'
	inner join OrderDetail on 'Order'.Id = OrderDetail.OrderId
	inner join Product on OrderDetail.ProductId = Product.Id
	inner join Customer on 'Order'.CustomerId = Customer.Id 
	where CompanyName = 'Queen Cozinha' and date(OrderDate) = '2014-12-25'
	order by Product.Id asc
),

Info2 as 
(
	select row_number() over (order by Info1.Id asc) as  NameNum, ProductName
	from Info1
),
Info3 as
(
	select NameNum, ProductName
	from Info2
	where NameNum = 1
	
	union all
	
	select Info2.NameNum, Info3.ProductName || ',' || Info2.ProductName
	from Info2 
	join Info3 on Info2.NameNum = Info3.NameNum + 1
)

select ProductName
from Info3
order by NameNum desc
limit 1
;	
