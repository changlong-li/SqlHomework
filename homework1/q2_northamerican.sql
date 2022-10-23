select Id, ShipCountry,
case 
when ShipCountry in ('USA', 'Mexico', 'Canada')
then 'NorthAmerica'
else 'OtherPlace'
end
from 'Order'
where Id >= 15445
order by Id
limit 20;
