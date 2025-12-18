import os
import time
import csv
import math
from typing import Dict, Any, Optional, Tuple, List

import pandas as pd
import requests
from sqlalchemy import (
    create_engine, Table, Column, Integer, String, Text, Float, MetaData,
    ForeignKey, UniqueConstraint, insert
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

# Configuration 
DB_PATH = "movies.db"
OMDB_API_KEY = "7a0a6224"  
OMDB_BASE_URL = "http://www.omdbapi.com/"
MOVIES_CSV = "movies.csv"
RATINGS_CSV = "ratings.csv"
REQUESTS_TIMEOUT = 15
OMDB_MAX_RETRIES = 2
OMDB_BACKOFF_SECONDS = 1.5
USE_OMDB = True # Set True to enable OMDb api call

def make_engine() -> Engine:
    # SQLite connection via SQLAlchemy
    return create_engine(f"sqlite:///{DB_PATH}", future=True)


def load_schema(engine: Engine):
    # Execute schema.sql once to ensure tables exist
    schema_file = os.path.join(os.getcwd(), "schema.sql")
    if not os.path.exists(schema_file):
        raise FileNotFoundError("schema.sql not found in workspace")
    with engine.begin() as conn, open(schema_file, "r", encoding="utf-8") as f:
        # Execute statements one by one (SQLite can't exec multiple at once)
        sql_text = f.read()
        for stmt in sql_text.split(";"):
            s = stmt.strip()
            if not s:
                continue
            conn.exec_driver_sql(s)


def parse_genres(genres_str: str) -> List[str]:
    if not isinstance(genres_str, str) or genres_str.strip() == "":
        return []
    # MovieLens uses '|' separator
    parts = [g.strip() for g in genres_str.split("|") if g.strip()]
    # Some datasets use '(no genres listed)'; skip it
    return [p for p in parts if p.lower() != "(no genres listed)"]


def compute_decade(year: Optional[int]) -> Optional[str]:
    if year is None:
        return None
    try:
        y = int(year)
    except Exception:
        return None
    base = (y // 10) * 10
    return f"{base}s"


def clean_box_office(box_office: Optional[str]) -> Optional[int]:
    # We'll store as dollars integer for simplicity
    if not box_office or not isinstance(box_office, str):
        return None
    s = box_office.strip()
    if s.upper() in {"N/A", ""}:
        return None
    s = s.replace("$", "").replace(",", "")
    try:
        return int(s)
    except ValueError:
        return None


def clean_runtime(runtime: Optional[str]) -> Optional[int]:
    if not runtime or not isinstance(runtime, str):
        return None
    s = runtime.lower().strip()
    if s.endswith(" min"):
        s = s.replace(" min", "")
    try:
        return int(s)
    except ValueError:
        return None


def omdb_fetch(title: str, year: Optional[int]) -> Dict[str, Any]:
    if not USE_OMDB or not OMDB_API_KEY:
        return {}
    # Clean title once: strip trailing parentheticals like "(1995)" or "(alias)"
    query_title = title if isinstance(title, str) else str(title or "")
    try:
        qt = query_title.strip()
        while qt.endswith(")") and "(" in qt:
            qt = qt.rsplit(" (", 1)[0]
        query_title = qt
    except Exception:
        query_title = (str(title) if title is not None else "").strip()

    params = {"apikey": OMDB_API_KEY, "t": query_title, "type": "movie", "r": "json"}
    if year:
        params["y"] = str(year)
    try:
        resp = requests.get(OMDB_BASE_URL, params=params, timeout=REQUESTS_TIMEOUT)
        if resp.status_code != 200:
            print(f"[WARN][omdb-http] title='{title}' q='{query_title}' year={year} status={resp.status_code}")
            return {}
        data = resp.json()
        if data.get("Response") != "True":
            print(f"[WARN][omdb-not-found] title='{title}' q='{query_title}' year={year} error='{data.get('Error')}'")
            return {}
        return data
    except Exception as e:
        print(f"[ERROR][omdb-exception] title='{title}' q='{query_title}' year={year} error='{e}'")
        return {}


def ensure_genres(conn, genres_table, genre_names: List[str]) -> Dict[str, int]:
    # Insert missing genres, return name->id map
    name_to_id: Dict[str, int] = {}
    if not genre_names:
        return name_to_id
    for name in genre_names:
        if not name:
            continue
        try:
            res = conn.execute(
                insert(genres_table).values(name=name)
            )

        except IntegrityError:
            pass
    placeholders = ",".join(["?" for _ in genre_names])
    rows = conn.exec_driver_sql(
        f"SELECT genre_id, name FROM genres WHERE name IN ({placeholders})",
        tuple(genre_names)
    ).fetchall()
    for gid, gname in rows:
        name_to_id[gname] = gid
    return name_to_id


def insert_movie(conn, movies_table, movie_record: Dict[str, Any]):
    cols = [
        "movie_id","title","release_year","decade","omdb_imdb_id",
        "omdb_director","omdb_plot","omdb_box_office","omdb_runtime_minutes"
    ]
    placeholders = ",".join(["?" for _ in cols])
    values = tuple(movie_record.get(c) for c in cols)
    conn.exec_driver_sql(
        f"INSERT INTO movies ({','.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT(movie_id) DO UPDATE SET "
        f"title=excluded.title, release_year=excluded.release_year, decade=excluded.decade, "
        f"omdb_imdb_id=excluded.omdb_imdb_id, omdb_director=excluded.omdb_director, "
        f"omdb_plot=excluded.omdb_plot, omdb_box_office=excluded.omdb_box_office, "
        f"omdb_runtime_minutes=excluded.omdb_runtime_minutes",
        values
    )


def insert_user(conn, users_table, user_id: int):
    conn.exec_driver_sql(
        "INSERT INTO users (user_id) VALUES (?) ON CONFLICT(user_id) DO NOTHING",
        (user_id,)
    )


def insert_rating(conn, ratings_table, rating_record: Dict[str, Any]):
    cols = ["user_id","movie_id","rating","rated_at"]
    placeholders = ",".join(["?" for _ in cols])
    values = tuple(rating_record.get(c) for c in cols)
    conn.exec_driver_sql(
        f"INSERT INTO ratings ({','.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT(user_id, movie_id) DO UPDATE SET rating=excluded.rating, rated_at=excluded.rated_at",
        values
    )


def link_movie_genres(conn, movie_id: int, genre_ids: List[int]):
    for gid in genre_ids:
        conn.exec_driver_sql(
            "INSERT INTO movie_genres (movie_id, genre_id) VALUES (?, ?) "
            "ON CONFLICT(movie_id, genre_id) DO NOTHING",
            (movie_id, gid)
        )


def extract_movies_and_ratings() -> Tuple[pd.DataFrame, pd.DataFrame]:
    movies_df = pd.read_csv(MOVIES_CSV)
    ratings_df = pd.read_csv(RATINGS_CSV)
    return movies_df, ratings_df


def transform_movie_row(row: pd.Series, omdb: Dict[str, Any]) -> Dict[str, Any]:
    title = str(row["title"]) if "title" in row else None
    # MovieLens titles often have " (Year)" suffix; extract release_year
    release_year = None
    if title and title.strip().endswith(")") and "(" in title:
        try:
            release_year = int(title.split("(")[-1].strip(")"))
        except Exception:
            release_year = None
    decade = compute_decade(release_year)
    def na_to_none(v: Any) -> Optional[str]:
        if v is None:
            return None
        if isinstance(v, str) and v.strip().upper() in {"N/A", ""}:
            return None
        return v
    omdb_imdb_id = na_to_none(omdb.get("imdbID"))
    omdb_director = na_to_none(omdb.get("Director"))
    omdb_plot = na_to_none(omdb.get("Plot"))
    omdb_box_office = clean_box_office(omdb.get("BoxOffice"))
    omdb_runtime_minutes = clean_runtime(omdb.get("Runtime"))
    return {
        "movie_id": int(row["movieId"]),
        "title": title,
        "release_year": release_year,
        "decade": decade,
        "omdb_imdb_id": omdb_imdb_id,
        "omdb_director": omdb_director,
        "omdb_plot": omdb_plot,
        "omdb_box_office": omdb_box_office,
        "omdb_runtime_minutes": omdb_runtime_minutes,
    }


def main():
    engine = make_engine()
    load_schema(engine)

    movies_df, ratings_df = extract_movies_and_ratings()

    metadata = MetaData()
    movies_table = Table("movies", metadata,
        Column("movie_id", Integer, primary_key=True),
        Column("title", Text, nullable=False),
        Column("release_year", Integer),
        Column("decade", String),
        Column("omdb_imdb_id", String),
        Column("omdb_director", Text),
        Column("omdb_plot", Text),
        Column("omdb_box_office", Integer),
        Column("omdb_runtime_minutes", Integer)
    )
    genres_table = Table("genres", metadata,
        Column("genre_id", Integer, primary_key=True),
        Column("name", String, unique=True, nullable=False)
    )
    movie_genres_table = Table("movie_genres", metadata,
        Column("movie_id", Integer, ForeignKey("movies.movie_id"), primary_key=True),
        Column("genre_id", Integer, ForeignKey("genres.genre_id"), primary_key=True)
    )
    users_table = Table("users", metadata,
        Column("user_id", Integer, primary_key=True)
    )
    ratings_table = Table("ratings", metadata,
        Column("user_id", Integer, ForeignKey("users.user_id"), primary_key=True),
        Column("movie_id", Integer, ForeignKey("movies.movie_id"), primary_key=True),
        Column("rating", Float, nullable=False),
        Column("rated_at", String)
    )

    movies_total = 0
    movies_ok = 0
    movies_err = 0
    genre_links_ok = 0
    genre_links_err = 0
    # Map duplicate movie_ids to canonical movie_id for ratings remap
    duplicate_map: Dict[int, int] = {}
    seen_title_year: Dict[Tuple[str, Optional[int]], int] = {}
    with engine.begin() as conn:
        for _, row in movies_df.iterrows():
            movies_total += 1
            title = str(row["title"]) if "title" in row else ""
            normalized_title = title.strip()
            # Parse year from title when present (e.g., "Toy Story (1995)")
            year = None
            if title and title.strip().endswith(")") and "(" in title:
                try:
                    year = int(title.split("(")[-1].strip(")"))
                except Exception:
                    year = None
            key = (normalized_title, year)

            movie_id_val = int(row["movieId"]) if "movieId" in row else None

            # If we've already seen this title+year, skip inserting and map duplicate
            if key in seen_title_year:
                canonical_id = seen_title_year[key]
                duplicate_map[movie_id_val] = canonical_id
                print(f"[SKIP][duplicate-title-year] movie_id={movie_id_val} title='{normalized_title}' year={year} -> canonical={canonical_id}")
                # Do not insert genres for duplicates to keep logic simple
                continue

            # First occurrence: insert movie (OMDb enrichment optional)
            omdb = omdb_fetch(title=title, year=year)
            movie_record = transform_movie_row(row, omdb)
            try:
                insert_movie(conn, movies_table, movie_record)
                seen_title_year[key] = movie_record["movie_id"]
                movies_ok += 1
            except Exception as e:
                movies_err += 1
                print(f"[ERROR][movie] movie_id={movie_record.get('movie_id')} title={movie_record.get('title')}: {e}")
                continue

            # Genres for the canonical movie
            genre_names = parse_genres(str(row["genres"])) if "genres" in row else []
            try:
                name_to_id = ensure_genres(conn, genres_table, genre_names)
                genre_ids = [name_to_id[g] for g in genre_names if g in name_to_id]
                link_movie_genres(conn, movie_record["movie_id"], genre_ids)
                genre_links_ok += len(genre_ids)
            except Exception as e:
                genre_links_err += 1
                print(f"[ERROR][genres] movie_id={movie_record.get('movie_id')} title={movie_record.get('title')}: {e}")

    # Process ratings with logging; isolate each insert to avoid poisoning the transaction
    ratings_total = 0
    ratings_ok = 0
    ratings_err = 0
    users_ok = 0
    users_err = 0
    canonical_ids = set(seen_title_year.values())
    with engine.connect() as conn:
        for _, rrow in ratings_df.iterrows():
            ratings_total += 1
            user_id = int(rrow["userId"])
            movie_id = int(rrow["movieId"])
            # Remap to canonical movie_id if this was a duplicate title+year
            if movie_id in duplicate_map:
                movie_id = duplicate_map[movie_id]
            # Skip if movie_id doesn't exist among canonical movies
            if movie_id not in canonical_ids:
                ratings_err += 1
                print(f"[SKIP][rating-missing-movie] user_id={user_id} movie_id={movie_id}")
                continue
            rating = float(rrow["rating"])
            ts = rrow.get("timestamp")
            rated_at = None
            if pd.notna(ts):
                try:
                    rated_at = pd.to_datetime(int(ts), unit="s").isoformat()
                except Exception:
                    rated_at = None
            trans = conn.begin()
            try:
                insert_user(conn, users_table, user_id)
                users_ok += 1
                insert_rating(conn, ratings_table, {
                    "user_id": user_id,
                    "movie_id": movie_id,
                    "rating": rating,
                    "rated_at": rated_at
                })
                ratings_ok += 1
                trans.commit()
            except Exception as e:
                trans.rollback()
                ratings_err += 1
                print(f"[ERROR][rating] user_id={user_id} movie_id={movie_id}: {e}")

    print("ETL completed successfully.")
    print(f"[SUMMARY] Movies processed={movies_total}, ok={movies_ok}, errors={movies_err}")
    print(f"[SUMMARY] Genre links ok={genre_links_ok}, errors={genre_links_err}")
    print(f"[SUMMARY] Ratings processed={ratings_total}, ok={ratings_ok}, errors={ratings_err}")
    print(f"[SUMMARY] Users ok={users_ok}, errors={users_err}")


if __name__ == "__main__":
    main()
