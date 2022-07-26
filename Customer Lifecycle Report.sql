//*The aim of this table is to help business identify a customer's behaviour 
by segmenting their status baised on their activity in each transaction month or lack thereof *//

-- This will be done using CTEs and a couple of analytical functions

select * from
(
WITH
order_sequence AS (

  SELECT
  order_date,
  customerid,
  customer,
  ROW_NUMBER() OVER (PARTITION BY customerid ORDER BY
  "date" ASC) as customer_order_sequence,
  LAG("date") OVER (PARTITION BY customerid ORDER BY
  "date" ASC) as previous_order_date
  FROM dbo_customer_transactions
  GROUP BY order_date, customerid
  ),

time_between_orders AS (

   SELECT
   order_date,
   customerid,
   customer_order_sequence,
   CASE WHEN previous_order_date IS NULL THEN order_date
   ELSE previous_order_date END AS previous_order_date,
   --extract(day from loadeddate - previous_loadeddate) AS
   datediff(day,  previous_loadeddate, "date")
   days_between_orders
   FROM order_sequence),
   
customer_life_cycle AS (

  SELECT
   order_date,
   customerid,
   CASE
   WHEN customer_order_sequence = 1 THEN 'New Customer' --this identifies the customer as a new customer.
   
   WHEN days_between_orders >= 0 AND days_between_orders <= 180 and customer_order_sequence <> 1
   --and LAG(days_between_orders) OVER (PARTITION BY customerid ORDER by "date" ASC) <= 180
   THEN 'Active Customer'
   
   /* it says, when their days between order is >= 0 and <=180 and they aren't new and the
     second ranked days between orders > 180, call them reactivated customers */
   --WHEN days_between_orders >= 0 AND days_between_orders <= 180 and customer_order_sequence <> 1
  -- and LAG(days_between_orders) OVER (PARTITION BY customerid ORDER by "date" ASC) > 180
   --THEN 'Reactivated Customer'
   WHEN days_between_orders > 180 then 'Reactivated Customer' --this identifies the customer as a reactivated or returned customer.
   ELSE 'Unknown'
   END AS sts,

   customer_order_sequence,
   previous_loadeddate,

   CASE
   WHEN days_between_orders IS NULL THEN 0
   ELSE days_between_orders
   END AS days_between_orders
   FROM time_between_orders),
aa as
(
select
max(t1.order_date) transaction_date,
date_part('month' , max(t1.order_date)) mon_no,
to_char(dateadd('month', 0, max(t1.order_date)), 'Mon-yyyy') mon_yr,
t1.customerid,
t2.days_between_orders,
t2.status,
max(t2.customer_order_sequence)customer_order_sequence,
t1.customer_country

FROM dbo_customer_transactions AS t1
LEFT JOIN customer_life_cycle AS t2
on (t1.customerid=t2.customerid
AND t1.order_date=t2.order_date)
group by 4,5,6,7,9
)
select *, row_number () over (partition by customerid, mon_yr order by dte asc, customerid) rnk
from aa
) a where rnk = 1 -- finally, selecting rank 1 picks the earliest transaction record from the table as a customer is expected to have multiple transactions in a month.