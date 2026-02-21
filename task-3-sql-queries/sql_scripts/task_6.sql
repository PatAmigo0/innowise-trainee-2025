SELECT 
	c.city,
	SUM(CASE WHEN cu.active = 1 THEN 1 ELSE 0 END) AS total_active,
	SUM(CASE WHEN cu.active = 0 THEN 1 ELSE 0 END) AS total_inactive
FROM customer AS cu
INNER JOIN address AS a
	ON a.address_id = cu.address_id
INNER JOIN city AS c
	ON c.city_id = a.city_id
GROUP BY c.city
ORDER BY total_inactive DESC;