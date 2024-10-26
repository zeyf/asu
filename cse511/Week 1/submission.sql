CREATE TABLE users (
	userid INT PRIMARY KEY,
	name VARCHAR NOT NULL
);

CREATE TABLE movies (
	movieid INT PRIMARY KEY,
	title VARCHAR NOT NULL
);

CREATE TABLE taginfo (
	tagid INT PRIMARY KEY,
	content VARCHAR NOT NULL
);

CREATE TABLE genres (
	genreid INT PRIMARY KEY,
	name VARCHAR NOT NULL
);

-- We can't have a rating for a movie by a user if the corresponding user or movie are deleted
-- A rating cannot exist without the user who makes it for a movie that exists
-- As for the primary key, a user can have a rating for different ratings, they just can't have more than one for a given movie!
CREATE TABLE ratings (
	userid INT NOT NULL,
	movieid INT NOT NULL,
	rating NUMERIC NOT NULL CHECK (rating > 0 AND rating <= 5),
	timestamp BIGINT NOT NULL,
	PRIMARY KEY (userid, movieid),
	FOREIGN KEY (userid) REFERENCES users(userid) ON DELETE CASCADE,
	FOREIGN KEY (movieid) REFERENCES movies(movieid) ON DELETE CASCADE
);

-- We can't have tags for a movie by a user if the corresponding tag, user, or movie are deleted
-- A tag cannot exist without the user who makes it for a movie that exists, and the corresponding content associated to it!
CREATE TABLE tags (
	userid INT NOT NULL,
	movieid INT NOT NULL,
	tagid INT NOT NULL,
	timestamp BIGINT NOT NULL,
	FOREIGN KEY (userid) REFERENCES users(userid) ON DELETE CASCADE,
	FOREIGN KEY (movieid) REFERENCES movies(movieid) ON DELETE CASCADE,
	FOREIGN KEY (tagid) REFERENCES taginfo(tagid) ON DELETE CASCADE
);

-- We can't say we have a genre for a movie if the corresponding genre or movie are deleted
-- Only a movie can have a genre and we need to know which movie has X genre... we need the movie and the genre to exist!
CREATE TABLE hasagenre (
	movieid INT NOT NULL,
	genreid INT NOT NULL,
	FOREIGN KEY (movieid) REFERENCES movies(movieid) ON DELETE CASCADE,
	FOREIGN KEY (genreid) REFERENCES genres(genreid) ON DELETE CASCADE
);