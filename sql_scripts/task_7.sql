WITH
cities_with_customers AS -- для более понятной логики, объединяем города с их кастомерами
(
	SELECT
		cu.customer_id,
		c.city_id,
		c.city
	FROM city AS c
	INNER JOIN address AS a
		ON a.city_id = c.city_id
	INNER JOIN customer AS cu
		ON cu.address_id = a.address_id
),
total_hours_table AS -- подсчитываем общее кол-во часов для всех категорий по усл.
(
	SELECT
		c.name,
		-- l_cities = letter cities = cities which starts with 'a'
		SUM(CASE WHEN cwc.city LIKE 'a%' THEN 
			ROUND(EXTRACT (EPOCH FROM (r.return_date - r.rental_date) / 3600), 2) ELSE 0 END) AS total_hours_rented_l_cities,
		-- s_cities = symbol cities = cities which contains symbol '-'
		SUM(CASE WHEN cwc.city LIKE '%-%' THEN 
			ROUND(EXTRACT (EPOCH FROM (r.return_date - r.rental_date) / 3600), 2) ELSE 0 END) AS total_hours_rented_s_cities
	FROM cities_with_customers AS cwc
	INNER JOIN rental AS r
		ON r.customer_id = cwc.customer_id
	INNER JOIN inventory AS i
		ON i.inventory_id = r.inventory_id
	INNER JOIN film_category AS fc
		ON fc.film_id = i.film_id
	INNER JOIN category AS c
		ON fc.category_id = c.category_id
	GROUP BY c.name
),
temp_null_table AS -- временная таблица с кучей NULL
(
	SELECT
		CASE WHEN DENSE_RANK() OVER (ORDER BY total_hours_rented_l_cities DESC) = 1 THEN name END AS c1,
		total_hours_rented_l_cities,
		CASE WHEN DENSE_RANK() OVER (ORDER BY total_hours_rented_s_cities DESC) = 1 THEN name END AS c2,
		total_hours_rented_s_cities
	FROM total_hours_table
)
SELECT
	MAX(c1) AS most_popular_ct_in_l_cities,
	MAX(total_hours_rented_l_cities) AS total_hours_rented_in_l_cities,
	MAX(c2) AS most_popular_ct_in_s_cities,
	MAX(total_hours_rented_s_cities) AS total_hours_rented_in_s_cities
FROM temp_null_table