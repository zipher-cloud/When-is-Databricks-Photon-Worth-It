SELECT p.product_name, SUM(t.amount) AS total_sales
FROM bronze.transactions_bronze t 
JOIN bronze.products_bronze p ON t.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_sales DESC;