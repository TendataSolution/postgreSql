/*Dataset : 
https://drive.google.com/file/d/1BLJqLxzJoVKuIs5fN3ugwmKaKwCOg53t/view?usp=sharing
https://drive.google.com/file/d/1X2MG9dOZjWoncVf1n7W8pM4qMEhg__1I/view?usp=sharing

The corporate strategy division of an airline company is currently reviewing pricing strategies in creating price layers
and requires information on ticket price preferences chosen by passengers.

By the corporate strategy division, they asked me to:

1. Make a price list based on departure and destination. From every
the existing price, for ranking (most expensive price = first ranking, second
the second most expensive ranking, etc.) based on the location of departure and
objective.

Query:*/
WITH pricelist as(
select distinct "from","to",price
from flight_dataset),

select *,
ROW_NUMBER() OVER(
PARTITION BY "from","to"
ORDER BY price desc) 
from pricelist

/*2. Get a price ranking of each flight transaction. Count the amount
flights in each price ranking for each
passenger.


Query:
*/
WITH pricelist as(
select distinct "from","to",price
from flight_dataset),
Rank_Route as
(select *,
ROW_NUMBER() OVER(
	PARTITION BY "from","to"
ORDER BY price desc) from pricelist)

select fd.user_id,Rr.row_number as rangking_harga,
count(travel_id) as jumlah_flight
from flight_dataset as fd
left join Rank_Route as Rr
on Rr."from" = fd."from"
AND Rr."to" = fd."to"
AND Rr.price = fd.price
group by fd.user_id,Rr.row_number


/*3. Get the ranking of the most traded by each
passenger.


Query:
*/
WITH pricelist as(
select distinct "from","to",price
from flight_dataset),
Rank_Route as
(select *,
ROW_NUMBER() OVER(
	PARTITION BY "from","to"
ORDER BY price desc) from pricelist),
flight_freq as
(select fd.user_id,Rr.row_number as rangking_harga,
count(travel_id) as jumlah_flight
from flight_dataset as fd
left join Rank_Route as Rr
on Rr."from" = fd."from"
AND Rr."to" = fd."to"
AND Rr.price = fd.price
group by fd.user_id,Rr.row_number)

select *, MAX(jumlah_flight) OVER(PARTITION BY user_id) as max_transaction
from flight_freq



/*4.Complete user data with rankings with the most prices
transacted by the passenger

Query:
*/
WITH pricelist as(
select distinct "from","to",price
from flight_dataset),
Rank_Route as
(select *,
ROW_NUMBER() OVER(
	PARTITION BY "from","to"
ORDER BY price desc) from pricelist),
flight_freq as
(select fd.user_id,Rr.row_number as price_rank,
count(travel_id) as jumlah_flight
from flight_dataset as fd
left join Rank_Route as Rr
on Rr."from" = fd."from"
AND Rr."to" = fd."to"
AND Rr.price = fd.price
group by fd.user_id,Rr.row_number),
Most_frequent as
(select *, MAX(jumlah_flight) OVER(PARTITION BY user_id) as max_transaction
from flight_freq)

select ud.*,mf.price_rank
from user_dataset as ud
left join Most_frequent as mf
ON ud.user_id = mf.user_id
and mf.jumlah_flight=mf.max_transaction






