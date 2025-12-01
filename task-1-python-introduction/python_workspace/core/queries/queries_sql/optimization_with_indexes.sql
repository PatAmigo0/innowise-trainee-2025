-- query for creating indexes

-- this index will contain information about all rooms in which there are men
CREATE INDEX IF NOT EXISTS idx_sex_rooms ON students(sex, room);

-- this index will contain information about room's name by id 
CREATE INDEX IF NOT EXISTS idx_id_name ON rooms(id, name);

-- making a tree with index room, which will contain information about all the students it has
CREATE INDEX IF NOT EXISTS idx_students_room_id ON students(room);

-- this index will contain information about every's student birthday in room
CREATE INDEX IF NOT EXISTS idx_students_room_id_birthday ON students(room, birthday);

-- this contains all sexes in the room
CREATE INDEX IF NOT EXISTS idx_students_room_sex ON students(room, sex);