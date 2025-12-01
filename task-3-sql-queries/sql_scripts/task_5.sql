WITH 
counted_table AS
(
	SELECT 
		a.actor_id,
		CONCAT(a.first_name, ' ', a.last_name) AS full_name,
		COUNT(*) AS total_children_ct_count
	FROM category AS c
	INNER JOIN film_category AS fc
		ON c.category_id = fc.category_id
	INNER JOIN film_actor AS fa
		ON fa.film_id = fc.film_id
	INNER JOIN actor AS a
		ON a.actor_id = fa.actor_id
	WHERE c.name = 'Children'
	GROUP BY a.actor_id, full_name
	ORDER BY total_children_ct_count DESC
),
ranked_table AS
(
	SELECT
		actor_id,
		full_name,
		DENSE_RANK() OVER (ORDER BY total_children_ct_count DESC) AS total_rank
	FROM counted_table
)
SELECT 
	actor_id,
	full_name
FROM ranked_table
WHERE total_rank <= 3
ORDER BY actor_id;