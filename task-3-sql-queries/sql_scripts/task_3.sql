SELECT 
	c.category_id,
	c.name,
	SUM(p.amount) AS total_earned
FROM film_category AS fc
INNER JOIN category AS c 
	ON c.category_id = fc.category_id
INNER JOIN film AS f 
	ON f.film_id = fc.film_id
INNER JOIN inventory AS i
	ON i.film_id = f.film_id 
INNER JOIN rental AS r
	ON r.inventory_id = i.inventory_id
INNER JOIN payment AS p
	ON p.rental_id = r.rental_id
GROUP BY c.category_id, c.name
ORDER BY total_earned DESC
LIMIT 1;
