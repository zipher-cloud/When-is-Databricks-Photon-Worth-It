SELECT p.category, YEAR(t.transaction_date) AS year, 
       MONTH(t.transaction_date) AS month, SUM(t.amount) AS monthly_sales
FROM bronze.transactions_bronze t 
JOIN bronze.products_bronze p ON t.product_id = p.product_id
GROUP BY p.category, YEAR(t.transaction_date), MONTH(t.transaction_date)
ORDER BY p.category, year, month;