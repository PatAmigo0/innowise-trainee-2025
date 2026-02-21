SELECT
	f.film_id,
	f.title
FROM film AS f
LEFT JOIN inventory AS i
	ON i.film_id = f.film_id
GROUP BY f.film_id, f.title
HAVING COUNT(i.inventory_id) = 0
ORDER BY f.film_id