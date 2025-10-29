SELECT
	c.category_id,
	c.name,
	COUNT(*) AS films_amount
FROM category AS c
INNER JOIN film_category AS fc 
	ON fc.category_id = c.category_id
GROUP BY c.category_id
ORDER BY films_amount DESC;