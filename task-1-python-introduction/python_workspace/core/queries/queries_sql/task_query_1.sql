SELECT 
	r.id, 
	r.name,
	COUNT(*) AS students_amount
FROM rooms AS r
INNER JOIN students AS s ON s.room = r.id
GROUP BY r.id, r.name
ORDER BY r.id