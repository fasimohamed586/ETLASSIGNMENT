1. Simple Movie Data Pipeline

This project loads movie data into a database and adds extra movie details using an external API.






2. What it does

Reads movie and rating data from CSV files (MovieLens dataset)

Fetches extra movie info from the OMDb API (director, plot, runtime, etc.)

Stores everything in a SQLite database

Can be run multiple times without creating duplicate data

Adds a few helpful fields like the movie’s decade

Lets you run SQL queries on the data




3. Tools Used

Python

pandas

SQLAlchemy

requests

SQLite database

Data Sources

MovieLens movies.csv and ratings.csv

OMDb API for extra movie details





4. Setup

Install Python 3.10 or newer

Create and activate a virtual environment

python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt






5. Make sure movies.csv and ratings.csv are in the project folder

6. Get an OMDb API key from
https://www.omdbapi.com/apikey.aspx






7. Configuration

All settings are inside etl.py:

Database file name

OMDb API key

CSV file paths




8. Run the Pipeline
python etl.py





9. Result:

A movies.db file is created

Movie, rating, user, and genre data is stored

Movie records are enriched with OMDb data when available

You can safely run the script again — it won’t insert duplicates.



10. Database Tables

movies – movie details and OMDb data

genres – list of genres

movie_genres – links movies to genres

users – user IDs

ratings – user ratings for movies





11. Run SQL Queries
queries.sql


12. Project Files
etl.py
schema.sql
queries.sql
requirements.txt
movies.csv
ratings.csv
README.md