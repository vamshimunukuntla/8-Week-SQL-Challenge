-- Databricks notebook source


-- COMMAND ----------

-- MAGIC
-- MAGIC %md
-- MAGIC ## Join Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SALES - MENU

-- COMMAND ----------

create or replace table sql_challenge.diner.sales_details as  
select 
  sales.*,
  menu.product_name,
  menu.price
from sql_challenge.diner.sales 
inner join sql_challenge.diner.menu 
on sales.product_id = menu.product_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SALES - MENU - MEMBERS

-- COMMAND ----------

create or replace table sql_challenge.diner.diner_data as
select 
  sales.*,
  menu.product_name,
  menu.price,
  members.join_date
from sql_challenge.diner.sales 
join sql_challenge.diner.menu 
on sales.product_id = menu.product_id
join sql_challenge.diner.members
on sales.customer_id = members.customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Load Tables

-- COMMAND ----------

select * from sql_challenge.diner.sales_details;

-- COMMAND ----------

select * from sql_challenge.diner.diner_data;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Default Catalog and Database

-- COMMAND ----------

USE CATALOG sql_challenge;
USE DATABASE diner;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Case Study Questions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1 . What is the total amount each customer spent at the restaurant?
-- MAGIC

-- COMMAND ----------

select
  customer_id,
  sum(price) as total_amount_spent
from sales_details
group by customer_id
order by total_amount_spent desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. How many days has each customer visited the restaurant?

-- COMMAND ----------

select  
  customer_id,
  count(distinct order_date) as visited_days
from sales_details
group by customer_id
order by visited_days desc;

-- COMMAND ----------

select  
  customer_id,
  count(product_id) as order_count
from sales
group by customer_Id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. What was the first item from the menu purchased by each customer?

-- COMMAND ----------

with cte as (
select 
  *,
  dense_rank() over(partition by customer_id order by order_date asc) as purchase_order_rank
from sales_details
)

select  
  customer_id,
  product_name
from cte
where purchase_order_rank = 1

-- COMMAND ----------

with cte as (
select 
  *,
  dense_rank() over(partition by customer_id order by order_date asc) as purchase_order_rank
from sales_details
)

select  
  customer_id,
  concat_ws(", ", collect_list(product_name)) as first_items_ordered
from cte
where purchase_order_rank = 1
group by customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. What is the most purchased item on the menu and how many times was it purchased by all customers?

-- COMMAND ----------

select 
  product_name,
  count(product_name) as purchase_count
from sales_details
group by product_name
order by purchase_count desc
limit 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5. Which item was the most popular for each customer?

-- COMMAND ----------

with cte as (
select  
  customer_id,
  product_name,
  count(*) as purchase_count
from sales_details
group by customer_id, product_name),

cte2 as (
select 
  *,
  dense_rank() over(partition by customer_id order by purchase_count desc) as product_purchase_rank
from cte)

select
  customer_id,
  product_name,
  purchase_count
from cte2
where product_purchase_rank = 1


-- COMMAND ----------

with cte as (
select  
  customer_id,
  product_name,
  count(*) as purchase_count
from sales_details
group by customer_id, product_name),

cte2 as (
select 
  *,
  dense_rank() over(partition by customer_id order by purchase_count desc) as product_purchase_rank
from cte),

cte3 as (
select
  customer_id,
  product_name,
  purchase_count
from cte2
where product_purchase_rank = 1
)

select  
  customer_id,
  string_agg(product_name, ', ') as product_name,
  string_agg(purchase_count, ', ') as purhcase_count
from cte3
group by customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6. Which item was purchased first by the customer after they became a member?

-- COMMAND ----------

with cte as (
select 
  *,
  dense_rank() over(partition by customer_id order by order_date asc) as order_rank
from diner_data
where order_date >= join_date
)
select  
  customer_id,
  product_name
from cte
where order_rank = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 7. Which item was purchased just before the customer became a member?

-- COMMAND ----------

with cte as (
select  
  *,
  dense_rank() over(partition by customer_id order by order_date desc) as order_rank
from diner_data
where order_date < join_date
)

select 
  customer_id, 
  product_name
from cte
where order_rank = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 8. What is the total items and amount spent for each member before they became a member?

-- COMMAND ----------

select  
  customer_id,
  count(product_name) as products_purchased,
  sum(price) as amount_spent
from diner_data
where order_date < join_date
group by customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 9. If each $1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?

-- COMMAND ----------

with cte as (
select 
  *,
  case 
    when product_name = 'sushi' then price * 10 * 2
    else price * 10
  end as points
from sales_details
)

select 
  customer_id,
  sum(points) as scoreboard
from cte
group by customer_id
order by scoreboard desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 10.In the first week after a customer joins the program (including their join date) they earn 2x points on all items, not just sushi - how many points do customer A and B have at the end of January? 

-- COMMAND ----------

with cte as (
select 
  *,
  case 
    when order_date >= join_date then 1 
    else 0 
  end as is_member,
  date_add(join_date, 6) as end_of_first_week,
  case 
    when order_date >= join_date and order_date <= end_of_first_week then 1
    else 0
  end as joining_bonus
from diner_data
where month(order_date) = 1
),

cte2 as (
select  
  *,
  case 
    when is_member = 1 and joining_bonus = 1 then price * 10 * 2
    when product_name = 'sushi' then price * 10 * 2
    else price * 10
  end as points
from cte
)

select 
  customer_id,
  sum(points) as scoreboard
from cte2
group by customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 11. Join all the things

-- COMMAND ----------

select 
  *
from sales
left join menu
on sales.product_id = menu.product_id
left join members
on sales.customer_id = members.customer_id

-- COMMAND ----------

select 
  sales.customer_id,
  sales.order_date,
  menu.product_name,
  menu.price,
  case 
    when join_date is null or order_date < join_date then "N"
    else "Y"
  end as member
from sales
left join menu
on sales.product_id = menu.product_id
left join members
on sales.customer_id = members.customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 12. Rank all the things

-- COMMAND ----------

with cte as (
select 
  sales.customer_id,
  sales.order_date,
  menu.product_name,
  menu.price,
  case 
    when join_date is null or order_date < join_date then "N"
    else "Y"
  end as member
from sales
left join menu
on sales.product_id = menu.product_id
left join members
on sales.customer_id = members.customer_id
)

select 
  *,
  case 
    when member = 'N' then null
    else dense_rank() over(partition by customer_id, member order by order_date) end as ranking
from cte
order by customer_Id

-- COMMAND ----------



-- COMMAND ----------

