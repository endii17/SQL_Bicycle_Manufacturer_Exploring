# SQL_Bicycle-Manufacturer-Dataset

## Table of Contents:
1. [Introduction](#clean_data)
2. [Goals](#clean_data)
3. [Import raw data](#cau3)
4. [Read and explain dataset](#cau4)
5. [Exploring the Dataset](#cau5)
6. [Conclusions](#cau6)


## 1. Introduction


## 2. Goals


## 3. Import raw data
The AdventureWorks dataset is stored in a public Google BigQuery dataset. To access the dataset, follow these steps:

Log in to your Google Cloud Platform account and create a new project.
Navigate to the BigQuery console and select your newly created project.
On Explorer, search "adventureworks2019" and open it.

## 4. Read and explain dataset
[AdventureWorks Data Dictionary](https://drive.google.com/file/d/1bwwsS3cRJYOg1cvNppc1K_8dQLELN16T/view)                                                                                                                                       |

## 5. Exploring the Dataset
In this project, I will write 08 queries in Bigquery for this dataset

### Query 01: Calculate Quantity of items, Sales value & Order quantity by each Subcategory in L12M

- SQL code
  
~~~~sql
with raw_data as (
        SELECT  
                DATE(s.ModifiedDate) as period
                ,p2.name
                ,SUM(OrderQty) as qty_item
                ,SUM(LineTotal) as total_sales
                ,COUNT (DISTINCT SalesOrderID) as order_cnt
        FROM `adventureworks2019.Sales.SalesOrderDetail` as s
        LEFT JOIN `adventureworks2019.Production.Product` as p1
        USING (ProductID)
        LEFT JOIN `adventureworks2019.Production.ProductSubcategory` as p2
        ON CAST(p1.ProductSubcategoryID as INT) = p2.ProductSubcategoryID
        WHERE DATE(s.ModifiedDate) between DATE_SUB(DATE '2014-06-30',INTERVAL 12 MONTH) 
                and DATE '2014-06-30'
        GROUP BY p2.name, period
)

SELECT 
       FORMAT_DATE('%b %Y', period) as period
       ,name
       ,SUM(qty_item) as qty_item
       ,SUM(total_sales) as total_sales
       ,SUM(order_cnt) as order_cnt
FROM raw_data
GROUP BY name, period
ORDER BY period DESC, name
~~~~
  
- Query result
  
| Row |  period   |  name             |  qty_item  |  total_sales       |  order_cnt  |
|-----|-----------|-------------------|------------|--------------------|-------------|
|   1 | Sep 2013  | Bike Racks        |        312 | 22828.512000000002 |          71 |
|   2 | Sep 2013  | Bike Stands       |         26 |             4134.0 |          26 |
|   3 | Sep 2013  | Bottles and Cages |        803 | 4676.5628080000006 |         380 |
|   4 | Sep 2013  | Bottom Brackets   |         60 | 3118.1400000000008 |          19 |
|   5 | Sep 2013  | Brakes            |        100 |             6390.0 |          29 |
|   6 | Sep 2013  | Caps              |        440 | 2879.4826160000002 |         203 |
|   7 | Sep 2013  | Chains            |         62 | 752.92800000000022 |          24 |
|   8 | Sep 2013  | Cleaners          |        296 | 1611.8307000000004 |         108 |
|   9 | Sep 2013  | Cranksets         |         75 |           13955.85 |          20 |
|  10 | Sep 2013  | Derailleurs       |         97 |  5972.069999999997 |          23 |

### Query 02: Calculate % YoY growth rate by SubCategory & release top 3 cat with highest grow rate. Can use metric: quantity_item. Round results to 2 decimal
- SQL code
  
~~~~sql
with raw_data as (
  SELECT 
        FORMAT_DATETIME('%Y',a.ModifiedDate) as year
        ,c.Name
        ,SUM(OrderQty) as qty_item
  FROM `adventureworks2019.Sales.SalesOrderDetail` as a
  LEFT JOIN `adventureworks2019.Production.Product` as b
  ON a.ProductID = b.ProductID
  LEFT JOIN `adventureworks2019.Production.ProductSubcategory` as c
  ON CAST(b.ProductSubcategoryID as INT) = c.ProductSubcategoryID
  GROUP BY c.name, year
  ORDER BY c.name, year
)

SELECT 
        name
        ,qty_item
        ,LAG(qty_item) OVER(PARTITION BY Name ORDER BY year) as prv_qty
        ,ROUND((qty_item - LAG(qty_item) OVER(PARTITION BY Name ORDER BY year))/LAG(qty_item) OVER(PARTITION BY Name ORDER BY year),2) as qty_diff
FROM raw_data 
ORDER BY qty_diff DESC
~~~~
  
- Query result
  
| Row |  name           |  qty_item  |  prv_qty  |  qty_diff  |
|-----|-----------------|------------|-----------|------------|
|   1 | Mountain Frames |       3168 |       510 |       5.21 |
|   2 | Socks           |       2724 |       523 |       4.21 |
|   3 | Road Frames     |       5564 |      1137 |       3.89 |
|   4 | Jerseys         |       4263 |      1027 |       3.15 |
|   5 | Helmets         |       4184 |      1032 |       3.05 |
|   6 | Caps            |       2048 |       545 |       2.76 |
|   7 | Road Bikes      |      19460 |      5342 |       2.64 |
|   8 | Shorts          |       5761 |      1586 |       2.63 |
|   9 | Mountain Bikes  |       9034 |      2621 |       2.45 |
|  10 | Jerseys         |      12104 |      4263 |       1.84 |

### Query 03: Ranking Top 3 TeritoryID with biggest Order quantity of every year. If there's TerritoryID with same quantity in a year, do not skip the rank number
- SQL code
  
~~~~sql
with raw_data as (
      SELECT 
            FORMAT_DATETIME('%Y',s1.ModifiedDate) as yr
            ,s2.TerritoryID
            ,sum(OrderQty) as order_cnt 
      FROM `adventureworks2019.Sales.SalesOrderDetail` as s1
      LEFT JOIN `adventureworks2019.Sales.SalesOrderHeader` as s2
      ON s1.SalesOrderID = s2.SalesOrderID
      GROUP BY yr, TerritoryID 
      ORDER BY yr DESC
)

,raw_data_1 as (
      SELECT *,
            dense_rank() OVER(PARTITION BY yr ORDER BY order_cnt DESC) as rk
      FROM raw_data
)

SELECT *
FROM raw_data_1
WHERE rk IN (1,2,3)
ORDER BY yr DESC
~~~~

- Query result

| Row |  yr   |  TerritoryID   |  order_cnt  |  rk  |
|-----|-------|----------------|-------------|------|
|   1 | 2014  |              4 |       11632 |    1 |
|   2 | 2014  |              6 |        9711 |    2 |
|   3 | 2014  |              1 |        8823 |    3 |
|   4 | 2013  |              4 |       26682 |    1 |
|   5 | 2013  |              6 |       22553 |    2 |
|   6 | 2013  |              1 |       17452 |    3 |
|   7 | 2012  |              4 |       17553 |    1 |
|   8 | 2012  |              6 |       14412 |    2 |
|   9 | 2012  |              1 |        8537 |    3 |
|  10 | 2011  |              4 |        3238 |    1 |

### Query 04: Calculate Total Discount Cost belongs to Seasonal Discount for each SubCategory
- SQL code
  
~~~~sql
SELECT  
      FORMAT_DATE('%Y', s1.ModifiedDate) as year
      ,p2.Name
      ,SUM((s2.DiscountPct*s1.UnitPrice*s1.OrderQty)) as total_cost
FROM `adventureworks2019.Sales.SalesOrderDetail` as s1
LEFT JOIN `adventureworks2019.Production.Product` as p1
USING (ProductID)
LEFT JOIN `adventureworks2019.Production.ProductSubcategory` as p2
ON CAST(p1.ProductSubcategoryID as INT) = p2.ProductSubcategoryID
LEFT JOIN `adventureworks2019.Sales.SpecialOffer` s2
ON s1.SpecialOfferID = s2.SpecialOfferID
WHERE s2.Type = 'Seasonal Discount'
GROUP BY year,Name
~~~~

- Query result

| Row |  year   |  Name   |  total_cost  |
|-----|---------|---------|--------------|
|   1 | 2012    | Helmets |    827.64732 |
|   2 | 2013    | Helmets |     1606.041 |

### Query 05: Retention rate of Customer in 2014 with status of Successfully Shipped (Cohort Analysis)
- SQL code
  
~~~~sql
with raw_data as (
      SELECT  
            extract(month from s1.ModifiedDate) as month_order
            ,extract(year from s1.ModifiedDate) as year
            ,CustomerID
            ,COUNT( DISTINCT s1.SalesOrderID) as sales_cnt
      FROM `adventureworks2019.Sales.SalesOrderDetail` as s1
      LEFT JOIN `adventureworks2019.Sales.SalesOrderHeader` as s2
      ON s1.SalesOrderID = s2.SalesOrderID
      WHERE s2.Status = 5 and extract(year from s1.ModifiedDate) = 2014
      GROUP BY s1.SalesOrderID, month_order, year, CustomerID
)

,raw_data_1 as (
      SELECT *
            ,row_number() OVER(PARTITION BY CustomerID ORDER BY month_order) as rn
      FROM raw_data
)

,first_buy_info as (
      SELECT 
            month_order as month_join,
            year,
            CustomerID
      FROM raw_data_1
      WHERE rn = 1
)

,join_data as (
      SELECT 
            month_order
            ,raw_data_1.year
            ,month_join
            ,CustomerID
            ,CONCAT('M','-', month_order - month_join) as month_diff
      FROM raw_data_1
      LEFT JOIN first_buy_info
      USING(CustomerID)
      ORDER BY CustomerID
)

SELECT 
      month_join,
      month_diff,
      COUNT(DISTINCT CustomerID) as customer_cnt
FROM join_data
GROUP BY month_join, month_diff
ORDER BY month_join, month_diff
~~~~

- Query result

| Row |  month_join  |  month_diff  |  customer_cnt  |
|-----|--------------|--------------|----------------|
|   1 |            1 | M-0          |           2073 |
|   2 |            1 | M-1          |             77 |
|   3 |            1 | M-2          |             92 |
|   4 |            1 | M-3          |             71 |
|   5 |            1 | M-4          |            268 |
|   6 |            1 | M-5          |             59 |
|   7 |            2 | M-0          |           1636 |
|   8 |            2 | M-1          |             58 |
|   9 |            2 | M-2          |             76 |
|  10 |            2 | M-3          |             71 |
  
### Query 06: Trend of Stock level & MoM diff % by all product in 2011. If %gr rate is null then 0. Round to 1 decimal
- SQL code
  
~~~~sql
with raw_data as (
  SELECT 
        p.name,
        extract (Month from w.EndDate) as month,
        extract (Year from w.EndDate) as year,
        SUM(w.StockedQty) as stock_qty
  FROM adventureworks2019.Production.Product as p
  LEFT JOIN adventureworks2019.Production.WorkOrder as w
  where FORMAT_TIMESTAMP("%Y", a.ModifiedDate) = '2011'   
  USING (ProductID)
  GROUP BY name, year, month
)

,mom_diff as (
  SELECT *,
        LEAD(stock_qty) OVER(PARTITION BY name ORDER BY month DESC) as stock_prv
  FROM raw_data
  --WHERE year = 2011
  ORDER BY name, month DESC
)

,raw_data_1 as (
  SELECT *
        ,ROUND(((stock_qty - stock_prv)/stock_prv)*100,1) as diff
  FROM mom_diff
)

SELECT name
      ,month
      ,year
      ,stock_qty
      ,stock_prv
      ,CASE WHEN diff is not NULL then diff  
            WHEN diff is NULL then 0 
      END as diff
FROM raw_data_1
~~~~

- Query result

| Row |  name           |  month  |  year   |  stock_qty  |  stock_prv  |  diff  |
|-----|-----------------|---------|---------|-------------|-------------|--------|
|   1 | BB Ball Bearing |      12 |    2011 |        8475 |       14544 |  -41.7 |
|   2 | BB Ball Bearing |      11 |    2011 |       14544 |       19175 |  -24.2 |
|   3 | BB Ball Bearing |      10 |    2011 |       19175 |        8845 |  116.8 |
|   4 | BB Ball Bearing |       9 |    2011 |        8845 |        9666 |   -8.5 |
|   5 | BB Ball Bearing |       8 |    2011 |        9666 |       12837 |  -24.7 |
|   6 | BB Ball Bearing |       7 |    2011 |       12837 |        5259 |  144.1 |
|   7 | BB Ball Bearing |       6 |    2011 |        5259 |        null |    0.0 |
|   8 | Blade           |      12 |    2011 |        1842 |        3598 |  -48.8 |
|   9 | Blade           |      11 |    2011 |        3598 |        4670 |  -23.0 |
|  10 | Blade           |      10 |    2011 |        4670 |        2122 |  120.1 |

### Query 07: Calculate Ratio of Stock / Sales in 2011 by product name, by month
- SQL code
  
~~~~sql
with 
sale_info as (
  select 
      extract(month from a.ModifiedDate) as mth 
     , extract(year from a.ModifiedDate) as yr 
     , a.ProductId
     , b.Name
     , sum(a.OrderQty) as sales
  from `adventureworks2019.Sales.SalesOrderDetail` a 
  left join `adventureworks2019.Production.Product` b 
    on a.ProductID = b.ProductID
  where FORMAT_TIMESTAMP("%Y", a.ModifiedDate) = '2011'
  group by 1,2,3,4
), 

stock_info as (
  select
      extract(month from ModifiedDate) as mth 
      , extract(year from ModifiedDate) as yr 
      , ProductId
      , sum(StockedQty) as stock_cnt
  from 'adventureworks2019.Production.WorkOrder'
  where FORMAT_TIMESTAMP("%Y", ModifiedDate) = '2011'
  group by 1,2,3
)

select
      a.*
    , coalesce(b.stock_cnt,0) as stock
    , round(coalesce(b.stock_cnt,0) / sales,2) as ratio
from sale_info a 
full join stock_info b 
  on a.ProductId = b.ProductId
and a.mth = b.mth 
and a.yr = b.yr
order by 1 desc, 7 desc
;
~~~~

- Query result

| Row |  mth   |  yr  |  ProductID  |  name                          |  sales  |  stock  |  ratio  |
|-----|--------|------|-------------|--------------------------------|---------|---------|---------|
|   1 |     12 | 2011 |         745 | HL Mountain Frame - Black, 48  |       1 |      27 |    27.0 |
|   2 |     12 | 2011 |         743 | HL Mountain Frame - Black, 42  |       1 |      26 |    26.0 |
|   3 |     12 | 2011 |         748 | HL Mountain Frame - Silver, 38 |       2 |      32 |    16.0 |
|   4 |     12 | 2011 |         722 | LL Road Frame - Black, 58      |       4 |      47 |    11.8 |
|   5 |     12 | 2011 |         747 | HL Mountain Frame - Black, 38  |       3 |      31 |    10.3 |
|   6 |     12 | 2011 |         726 | LL Road Frame - Red, 48        |       5 |      36 |     7.2 |
|   7 |     12 | 2011 |         738 | LL Road Frame - Black, 52      |      10 |      64 |     6.4 |
|   8 |     12 | 2011 |         730 | LL Road Frame - Red, 62        |       7 |      38 |     5.4 |
|   9 |     12 | 2011 |         741 | HL Mountain Frame - Silver, 48 |       5 |      27 |     5.4 |
|  10 |     12 | 2011 |         725 | LL Road Frame - Red, 44        |      12 |      53 |     4.4 |


### Query 08: No of order and value at Pending status in 2014

- SQL code
~~~~sql
with raw_data as (
  SELECT 
        extract(year from ShipDate) as yr
        ,COUNT(DISTINCT PurchaseOrderID) as order_Cnt 
        ,SUM(TotalDue) as value
  FROM adventureworks2019.Purchasing.PurchaseOrderHeader
  WHERE status = 1
  GROUP BY yr
)

SELECT *
FROM raw_data
WHERE yr = 2014
~~~~

- Query result

| Row |  yr   |  order_Cnt   |  value             |
|-----|-------|--------------|--------------------|
|   1 |  2014 |          224 | 3873579.0123000029 |

# 6. Conclusions

