SELECT p.category, AVG(t.amount) AS avg_high_value_amount
FROM bronze.transactions_bronze t 
JOIN bronze.products_bronze p ON t.product_id = p.product_id
WHERE t.amount > 500
GROUP BY p.category
ORDER BY avg_high_value_amount DESC;