select RegionDescription, FirstName, LastName, BirthDate
from 
(
	select RegionDescription, FirstName, LastName, BirthDate, max(julianday(BirthDate)), RegionId
	from Employee
	inner join EmployeeTerritory on EmployeeTerritory.EmployeeId = Employee.Id
	inner join Territory on Territory.Id = EmployeeTerritory.TerritoryId
	inner join Region on Region.Id = Territory.RegionId
	group by Region.Id
)
order by RegionId
;
