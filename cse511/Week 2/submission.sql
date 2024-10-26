
-- Write an SQL query to return the total number of movies for each genre. Your query result
-- should be saved in a table called “query1” which has two attributes: “name” attribute is a list of
-- genres, and “moviecount” attribute is a list of movie counts for each genre.
CREATE TABLE query1 AS (
	SELECT
		g.name,
		COUNT(*) AS moviecount
	FROM
		genres g,
		movies m,
		hasagenre hag
	WHERE (
		hag.movieid = m.movieid
		AND hag.genreid = g.genreid
	)
	GROUP BY g.name
);

-- Write an SQL query to return the average rating per genre. Your query result should be saved
-- in a table called “query2” which has two attributes: “name” attribute is a list of all genres, and
-- “rating” attribute is a list of average rating per genre.
CREATE TABLE query2 AS (
	SELECT
		g.name,
		AVG(r.rating) AS rating
	FROM
		genres g,
		hasagenre hag,
		ratings r
	WHERE (
		hag.movieid = r.movieid
		AND hag.genreid = g.genreid
	)
	GROUP BY g.name
);

-- Write an SQL query to return the movies that have at least 10 ratings. Your query result should
-- be saved in a table called “query3” which has two attributes: “title” is a list of movie titles, and
-- “countofratings” is a list of ratings.
CREATE TABLE query3 AS (
	SELECT
		m.title,
		COUNT(*) AS countofratings
	FROM
		movies m,
		ratings r
	WHERE
		m.movieid = r.movieid
	GROUP BY m.title
	HAVING (COUNT(*) >= 10)
);


-- Write a SQL query to return all “Comedy” movies, including movieid and title. Your query result
-- should be saved in a table called “query4” which has two attributes: “movieid” is a list of
-- movie ids, and “title” is a list of movie titles.
CREATE TABLE query4 AS (
	SELECT
		m.movieid,
		m.title
	FROM
		movies m,
		genres g,
		hasagenre hag
	WHERE
		m.movieid = hag.movieid
		AND g.genreid = hag.genreid
		AND g.name = 'Comedy'
);

-- Write an SQL query to return the average rating per movie. Your query result should be saved
-- in a table called “query5” which has two attributes: “title” is a list of movie titles, and “average”
-- is a list of the average rating per movie.
CREATE TABLE query5 AS (
	SELECT
		m.title,
		AVG(r.rating) AS average
	FROM
		movies m,
		ratings r
	WHERE
		m.movieid = r.movieid
	GROUP BY m.title
);

-- Write a SQL query to return the average rating for all “Comedy” movies. Your query result
-- should be saved in a table called “query6” with one attribute: “average”.
CREATE TABLE query6 AS (
	SELECT
		AVG(r.rating) AS average
	FROM
		ratings r,
		genres g,
		hasagenre hag
	WHERE
		r.movieid = hag.movieid
		AND g.genreid = hag.genreid
		AND g.name = 'Comedy'
);

-- Write a SQL query to return the average rating for all movies and each of these movies is both
-- “Comedy” and “Romance”. Your query result should be saved in a table called “query7” which
-- has one attribute: “average”.
CREATE TABLE query7 AS (
	SELECT
		AVG(r.rating) AS average
	FROM
		ratings r,
		genres g1,
		hasagenre hag1,
		genres g2,
		hasagenre hag2
	WHERE
		r.movieid = hag1.movieid
		AND g1.genreid = hag1.genreid
		AND g1.name = 'Comedy'
		AND r.movieid = hag2.movieid
		AND g2.genreid = hag2.genreid
		AND g2.name = 'Romance'
);

-- Write a SQL query to return the average rating for all movies and each of these movies is
-- “Romance” but not “Comedy”. Your query result should be saved in a table called “query8”
-- which has one attribute: “average”.
CREATE TABLE query8 AS (
	SELECT
		AVG(r.rating) AS average
	FROM
		ratings r,
		genres g1,
		hasagenre hag1
	WHERE
		r.movieid = hag1.movieid
		AND g1.genreid = hag1.genreid
		AND g1.name = 'Romance'
		AND r.movieid NOT IN (
			SELECT
				hag2.movieid
			FROM
				genres g2,
				hasagenre hag2
			WHERE
				g2.genreid = hag2.genreid
				AND g2.name = 'Comedy'
		)
);

-- Find all movies that are rated by a user such that the userId is equal to v1. The v1 will be an
-- integer parameter passed to the SQL query. Your query result should be saved in a table
-- called “query9” which has two attributes: “movieid” is a list of movieid’s rated by userId v1,
-- and “rating” is a list of ratings given by userId v1 for corresponding movieid.
CREATE TABLE query9 AS (
	SELECT
		movieid,
		rating
	FROM
		ratings r
	WHERE
		r.userid = :v1
);