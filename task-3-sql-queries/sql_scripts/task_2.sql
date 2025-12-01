SELECT 
	a.actor_id,
	CONCAT(a.first_name, ' ', a.last_name) AS full_name,
	COUNT(*) AS rentals_amount
FROM film_actor AS fa
INNER JOIN inventory AS i
	ON i.film_id = fa.film_id
INNER JOIN actor AS a
	ON a.actor_id = fa.actor_id
GROUP BY a.actor_id, full_name
ORDER BY rentals_amount DESC
LIMIT 10;