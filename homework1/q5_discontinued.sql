select ProductName, CompanyName, ContactName
from 
	(select ProductName, CompanyName, ContactName, min(OrderDate), discontinued
	from Product
	join OrderDetail on OrderDetail.ProductId = Product.Id
	join 'Order' on 'Order'.Id = OrderDetail.OrderId
	join Customer on 'Order'.CustomerId = Customer.Id
	group by ProductName)
where discontinued = 1
order by ProductName asc
;
