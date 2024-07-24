--q1
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

--q2
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


--q3
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


--q4
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


--q5
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


--q6
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


--q7
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

--q8
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



                                            --very good---