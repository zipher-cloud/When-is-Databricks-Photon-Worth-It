SELECT u.country, SUM(t.amount) AS total_spending
FROM bronze.transactions_bronze t 
JOIN bronze.users_bronze u ON t.user_id = u.user_id
GROUP BY u.country
ORDER BY total_spending DESC;