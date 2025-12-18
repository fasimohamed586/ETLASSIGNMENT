-- 1. Which movie has the highest average rating?
SELECT m.movie_id, m.title, ROUND(AVG(r.rating), 3) AS avg_rating,
       COUNT(r.user_id) AS rating_count
FROM movies m
JOIN ratings r ON r.movie_id = m.movie_id
GROUP BY m.movie_id, m.title
ORDER BY avg_rating DESC, rating_count DESC
LIMIT 1;

-- 2. Top 5 movie genres with highest average rating
SELECT g.name AS genre, ROUND(AVG(r.rating), 3) AS avg_rating,
       COUNT(*) AS rating_count
FROM ratings r
JOIN movie_genres mg ON mg.movie_id = r.movie_id
JOIN genres g ON g.genre_id = mg.genre_id
GROUP BY g.genre_id, g.name
HAVING COUNT(*) >= 10
ORDER BY avg_rating DESC, rating_count DESC
LIMIT 5;

-- 3. Director with the most movies in this dataset
SELECT COALESCE(m.omdb_director, '(Unknown)') AS director,
       COUNT(*) AS movie_count
FROM movies m
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;

-- 4. Average rating of movies released each year
SELECT m.release_year, ROUND(AVG(r.rating), 3) AS avg_rating,
       COUNT(*) AS rating_count
FROM movies m
JOIN ratings r ON r.movie_id = m.movie_id
WHERE m.release_year IS NOT NULL
GROUP BY m.release_year
ORDER BY m.release_year ASC;

