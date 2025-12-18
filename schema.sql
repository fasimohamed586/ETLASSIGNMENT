
PRAGMA foreign_keys = ON;

-- Reset schema to avoid mismatched prior definitions

DROP TABLE IF EXISTS movie_genres;
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS genres;
DROP TABLE IF EXISTS movies;

CREATE TABLE IF NOT EXISTS movies (
  movie_id INTEGER PRIMARY KEY, 
  title TEXT NOT NULL,
  release_year INTEGER,
  decade TEXT, 
  omdb_imdb_id TEXT,
  omdb_director TEXT,
  omdb_plot TEXT,
  omdb_box_office INTEGER,
  omdb_runtime_minutes INTEGER,
  UNIQUE(movie_id)
);

CREATE TABLE IF NOT EXISTS genres (
  genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS movie_genres (
  movie_id INTEGER NOT NULL,
  genre_id INTEGER NOT NULL,
  PRIMARY KEY (movie_id, genre_id),
  FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE,
  FOREIGN KEY (genre_id) REFERENCES genres(genre_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,
  UNIQUE(user_id)
);

CREATE TABLE IF NOT EXISTS ratings (
  user_id INTEGER NOT NULL,
  movie_id INTEGER NOT NULL,
  rating REAL NOT NULL,
  rated_at TEXT,
  PRIMARY KEY (user_id, movie_id),
  FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
  FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE
);
