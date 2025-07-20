WITH ranked_users AS (
  SELECT u.country, t.user_id, SUM(t.amount) AS total_spending,
         RANK() OVER (PARTITION BY u.country ORDER BY SUM(t.amount) DESC) AS spending_rank
  FROM bronze.transactions_bronze t
  JOIN bronze.users_bronze u ON t.user_id = u.user_id
  GROUP BY u.country, t.user_id
)
SELECT * FROM ranked_users WHERE spending_rank <= 5;