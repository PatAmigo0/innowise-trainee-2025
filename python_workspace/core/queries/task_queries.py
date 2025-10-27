query_student_count_by_room = """
SELECT r.name, COUNT(s.id) as student_count
FROM rooms r
LEFT JOIN students s ON s.room = r.id
GROUP BY r.id, r.name
ORDER BY student_count DESC, r.name;
"""

query_rooms_lowest_avg_age = """
SELECT
    r.name,
    AVG(EXTRACT(YEAR FROM AGE(s.birthday))) as average_age
FROM rooms r
JOIN students s ON s.room = r.id
GROUP BY r.id, r.name
HAVING COUNT(s.id) > 0
ORDER BY average_age ASC
LIMIT 5;
"""

query_rooms_max_age_difference = """
SELECT
    r.name,
    (MAX(AGE(s.birthday)) - MIN(AGE(s.birthday))) as age_difference
FROM rooms r
JOIN students s ON s.room = r.id
GROUP BY r.id, r.name
HAVING COUNT(s.id) > 1
ORDER BY age_difference DESC
LIMIT 5;
"""

query_rooms_mixed_gender = """
SELECT r.name
FROM rooms r
JOIN students s ON s.room = r.id
GROUP BY r.id, r.name
HAVING COUNT(DISTINCT s.sex) > 1;
"""
