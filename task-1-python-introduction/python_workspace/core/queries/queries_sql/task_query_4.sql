WITH SexCount AS
(
	-- counting any possible sex (default is nonbin)
	SELECT
		r.id,
		r.name,
		COUNT(CASE WHEN s.sex = 'M' THEN 1 END) AS male_count,
		COUNT(CASE WHEN s.sex = 'F' THEN 1 END) AS female_count,
		COUNT(CASE WHEN s.sex != 'M' AND s.sex != 'F' THEN 1 END) AS nonbin_count,
		COUNT(*) AS students_amount
	FROM rooms AS r
	INNER JOIN students AS s on s.room = r.id
	GROUP BY r.id, r.name
)
-- finalizing by using where to filter values 
SELECT 
	id, 
	name,
	male_count,
	female_count,
	nonbin_count,
	students_amount
FROM SexCount
WHERE male_count != 0 AND (female_count != 0 OR nonbin_count != 0) 
ORDER BY id;