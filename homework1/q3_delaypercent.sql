select distinct Shipper.CompanyName, LateRate 
from Shipper,
	(select A.ShipVia, round((1.0 * A.late / B.cnt) * 100, 2) as LateRate from
		(select ShipVia, count(Id) as late from 'Order' where ShippedDate > RequiredDate group by ShipVia) as A,
		(select ShipVia, count(Id) as cnt from 'Order' group by ShipVia) as B
	where A.ShipVia = B.ShipVia) as R
where Shipper.Id = R.ShipVia
order by LateRate desc;
