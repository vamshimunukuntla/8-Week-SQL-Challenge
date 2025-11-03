# 🍜 🍛 🍣 Case Study #1: Danny's Diner

![Logo](https://8weeksqlchallenge.com/images/case-study-designs/1.png)

Introduction
-
Danny seriously loves Japanese food so in the beginning of 2021, he decides to embark upon a risky venture and opens up a cute little restaurant that sells his 3 favourite foods: sushi, curry and ramen.

Danny’s Diner is in need of your assistance to help the restaurant stay afloat - the restaurant has captured some very basic data from their few months of operation but have no idea how to use their data to help them run the business.

Problem Statement
-
Danny wants to use the data to answer a few simple questions about his customers, especially about their visiting patterns, how much money they’ve spent and also which menu items are their favourite. Having this deeper connection with his customers will help him deliver a better and more personalised experience for his loyal customers. He plans on using these insights to help him decide whether he should expand the existing customer loyalty program.

Datasets used
-
Three key datasets for this case study

- sales: The sales table captures all customer_id level purchases with an corresponding order_date and product_id information for when and what menu items were ordered.
- menu: The menu table maps the product_id to the actual product_name and price of each menu item.
- members: The members table captures the join_date when a customer_id joined the beta version of the Danny’s Diner loyalty program.

Entity Relationship Diagram
-
![Alt text]( "Danny's Diner ERD Diagram")






# Solution

## Create `sales_details` table

Tables used:
- sales 
- menu

```sql
create or replace table sql_challenge.diner.sales_details as  
select 
  sales.*,
  menu.product_name,
  menu.price
from sql_challenge.diner.sales 
inner join sql_challenge.diner.menu 
on sales.product_id = menu.product_id;
```

## Create `diner_data` table

Tables used:
- sales 
- menu
- members

```sql
create or replace table sql_challenge.diner.diner_data as
select 
  sales.*,
  menu.product_name,
  menu.price,
  members.join_date
from sql_challenge.diner.sales 
inner join sql_challenge.diner.menu 
on sales.product_id = menu.product_id
inner join sql_challenge.diner.members
on sales.customer_id = members.customer_id;
```

## Make it default!

```sql
USE CATALOG sql_challenge;
USE SCHEMA diner;
```

### 1 . What is the total amount each customer spent at the restaurant?

```sql
select
  customer_id,
  sum(price) as total_amount_spent
from sales_details
group by customer_id
order by total_amount_spent desc
```

#### Output:

| customer_id | total_amount_spent |
|--------------|--------------------|
| A            | 76                 |
| B            | 74                 |
| C            | 36                 |


### 2. How many days has each customer visited the restaurant?


```sql
select  
  customer_id,
  count(distinct order_date) as visited_days
from sales_details
group by customer_id
order by visited_days desc;
```

#### Output:

| customer_id | visited_days |
|--------------|--------------|
| B            | 6            |
| A            | 4            |
| C            | 2            |


### 3. What was the first item from the menu purchased by each customer?

- #### Version 1:

```sql
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
```

| customer_id | product_name |
| ----------- | ------------ |
| A           | sushi        |
| A           | curry        |
| B           | curry        |
| C           | ramen        |
| C           | ramen        |

- #### Version 2:

```sql
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
group by customer_id;
```
#### Output:

| customer_id | first_items_ordered |
|--------------|---------------------|
| A            | sushi, curry        |
| B            | curry               |
| C            | ramen, ramen        |


### 4. What is the most purchased item on the menu and how many times was it purchased by all customers?

```sql
select 
  product_name,
  count(product_name) as purchase_count
from sales_details
group by product_name
order by purchase_count desc
limit 1
```
#### Output:

| product_name | purchase_count |
|---------------|----------------|
| ramen         | 8              |

### 5. Which item was the most popular for each customer?

- #### Version 1:

```sql
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
```

#### Output:

| customer_id | product_name | purchase_count |
|--------------|--------------|----------------|
| A            | ramen        | 3              |
| B            | curry        | 2              |
| B            | sushi        | 2              |
| B            | ramen        | 2              |
| C            | ramen        | 3              |

- #### Version 2:

```sql
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
```

#### Output:

| customer_id | product_name           | purhcase_count |
|--------------|------------------------|----------------|
| A            | ramen                  | 3              |
| B            | curry, sushi, ramen    | 2, 2, 2        |
| C            | ramen                  | 3              |


### 6. Which item was purchased first by the customer after they became a member?

```sql
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
```

#### Output:

| customer_id | product_name |
|--------------|--------------|
| A            | curry        |
| B            | sushi        |

### 7. Which item was purchased just before the customer became a member?

```sql
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
where order_rank = 1;
```

#### Output:

| customer_id | product_name |
|--------------|--------------|
| A            | sushi        |
| A            | curry        |
| B            | sushi        |


### 8. What is the total items and amount spent for each member before they became a member?

```sql
select  
  customer_id,
  count(product_name) as products_purchased,
  sum(price) as amount_spent
from diner_data
where order_date < join_date
group by customer_id
```

#### Output:
| customer_id | products_purchased | amount_spent |
|--------------|--------------------|---------------|
| A            | 2                  | 25            |
| B            | 3                  | 40            |

### 9. If each $1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?

```sql
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
```
| customer_id | scoreboard |
|--------------|------------|
| B            | 940        |
| A            | 860        |
| C            | 360        |

#### Output:


### 10.In the first week after a customer joins the program (including their join date) they earn 2x points on all items, not just sushi - how many points do customer A and B have at the end of January?

```sql
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
```

#### Output:

| customer_id | scoreboard |
|--------------|------------|
| A            | 1370       |
| B            | 820        |

### 11. Join all the things

```sql
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
```

#### Output:

| customer_id | order_date | product_name | price | member |
|--------------|-------------|---------------|--------|---------|
| A            | 2021-01-01  | sushi         | 10     | N       |
| A            | 2021-01-01  | curry         | 15     | N       |
| A            | 2021-01-07  | curry         | 15     | Y       |
| A            | 2021-01-10  | ramen         | 12     | Y       |
| A            | 2021-01-11  | ramen         | 12     | Y       |
| A            | 2021-01-11  | ramen         | 12     | Y       |
| B            | 2021-01-01  | curry         | 15     | N       |
| B            | 2021-01-02  | curry         | 15     | N       |
| B            | 2021-01-04  | sushi         | 10     | N       |
| B            | 2021-01-11  | sushi         | 10     | Y       |
| B            | 2021-01-16  | ramen         | 12     | Y       |
| B            | 2021-02-01  | ramen         | 12     | Y       |
| C            | 2021-01-01  | ramen         | 12     | N       |
| C            | 2021-01-01  | ramen         | 12     | N       |
| C            | 2021-01-07  | ramen         | 12     | N       |

### 12. Rank all the things

```sql
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
order by customer_id
```

#### Output:

| customer_id | order_date | product_name | price | member | ranking |
|--------------|-------------|---------------|--------|---------|----------|
| A            | 2021-01-01  | sushi         | 10     | N       | null     |
| A            | 2021-01-01  | curry         | 15     | N       | null     |
| A            | 2021-01-07  | curry         | 15     | Y       | 1        |
| A            | 2021-01-10  | ramen         | 12     | Y       | 2        |
| A            | 2021-01-11  | ramen         | 12     | Y       | 3        |
| A            | 2021-01-11  | ramen         | 12     | Y       | 3        |
| B            | 2021-01-01  | curry         | 15     | N       | null     |
| B            | 2021-01-02  | curry         | 15     | N       | null     |
| B            | 2021-01-04  | sushi         | 10     | N       | null     |
| B            | 2021-01-11  | sushi         | 10     | Y       | 1        |
| B            | 2021-01-16  | ramen         | 12     | Y       | 2        |
| B            | 2021-02-01  | ramen         | 12     | Y       | 3        |
| C            | 2021-01-01  | ramen         | 12     | N       | null     |
| C            | 2021-01-01  | ramen         | 12     | N       | null     |
| C            | 2021-01-07  | ramen         | 12     | N       | null     |



