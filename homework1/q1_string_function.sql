select distinct ShipName, substr(distinct ShipName, 1, instr(distinct ShipName, "-")-1) from 'Order' Ord where Ord.ShipName like "%-%" order by ShipName ;


