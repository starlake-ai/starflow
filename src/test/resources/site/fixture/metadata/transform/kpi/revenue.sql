SELECT c.id AS customer_id, SUM(o.amount * fx.rate) AS revenue
FROM sales.orders o
JOIN sales.customers c ON o.customer_id = c.id
JOIN refdata.fx_rates fx ON fx.currency = 'EUR'
GROUP BY c.id
