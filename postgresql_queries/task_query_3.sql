WITH RankedStudents AS 
(
	-- looking for oldest and youngest
	SELECT
		r.id,
		r.name,
		s.birthday,
		RANK() OVER (PARTITION BY r.id ORDER BY s.birthday DESC) AS youngest_rank,
		RANK() OVER (PARTITION BY r.id ORDER BY s.birthday ASC) AS oldest_rank
	FROM rooms AS r
	INNER JOIN students AS s ON s.room = r.id
),
TransformedStudents AS
(
	-- extracting their ages
	SELECT 
		id,
		name,
		MAX(CASE WHEN youngest_rank = 1 THEN EXTRACT (YEAR FROM AGE(birthday)) END) AS youngest_age,
		MAX(CASE WHEN oldest_rank = 1 THEN EXTRACT (YEAR FROM AGE(birthday)) END) AS oldest_age
	FROM RankedStudents
	GROUP BY id, name
)
-- finalizing
SELECT
	id,
	name,
	youngest_age,
	oldest_age,
	oldest_age - youngest_age AS age_difference
FROM TransformedStudents
ORDER BY oldest_age - youngest_age DESC
LIMIT 5;