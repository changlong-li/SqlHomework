select CategoryName, Nump, Avep, Minp, Maxp, TotOrd
from Category,
	(select CategoryId, count(*) as Nump, round(avg(UnitPrice), 2) as Avep, min(UnitPrice) as Minp, max(UnitPrice) as Maxp, sum(UnitsOnOrder) as TotOrd
	from Product
	group by CategoryId 
	having Nump > 10)

where Id = CategoryId
order by CategoryId
;
