SELECT 
	r.id, 
	r.name,
	AVG(EXTRACT (YEAR FROM AGE(s.birthday)))::int AS avg_age -- using int instead of floor cuz it looks cool
FROM rooms AS r
INNER JOIN students AS s on s.room = r.id
GROUP BY r.id, r.name
ORDER BY avg_age ASC
LIMIT 5;