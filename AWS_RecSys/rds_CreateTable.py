#%%
import mysql.connector
#%%
# Database Connection (adjust for your RDS MySQL instance)
conn = mysql.connector.connect(
    user='admin', password='your-password', 
    host='database.cffidgh00pge.us-east-1.rds.amazonaws.com', 
    database='mydb'
)
cur = conn.cursor()


#%%
#Create table, user, movie, rating in RDS in database mydb
user_table = """
CREATE TABLE IF NOT EXISTS User (
    userid INT PRIMARY KEY,
    age FLOAT,
    occupation VARCHAR(255),
    gender VARCHAR(50)
);
"""
rating_table = """
CREATE TABLE IF NOT EXISTS Rating (
    userid INT,
    movieid VARCHAR(255),
    rating FLOAT,
    rating_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(userid, movieid),
    FOREIGN KEY(userid) REFERENCES User(userid) ON DELETE CASCADE,
    FOREIGN KEY(movieid) REFERENCES Movie(movieid) ON DELETE CASCADE
);
"""
watch_table = """
CREATE TABLE IF NOT EXISTS Watch (
    userid INT,
    movieid VARCHAR(255),
    movieparts FLOAT,
    watch_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(userid, movieid),
    FOREIGN KEY(userid) REFERENCES User(userid) ON DELETE CASCADE,
    FOREIGN KEY(movieid) REFERENCES Movie(movieid) ON DELETE CASCADE,
);
"""
movie_table  = """
CREATE TABLE IF NOT EXISTS Movie (
    movieid VARCHAR(255) PRIMARY KEY,
    tmdb_id VARCHAR(255),
    imdb_id VARCHAR(255),
    title VARCHAR(255),
    original_title VARCHAR(255),
    adult BOOLEAN,
    belongs_to_collection VARCHAR(255),
    budget FLOAT,
    genre1 VARCHAR(50),
    genre2 VARCHAR(50),
    genre3 VARCHAR(50),
    homepage VARCHAR(255),
    original_language VARCHAR(50),
    overview TEXT,
    popularity FLOAT,
    poster_path VARCHAR(255),
    production_companies VARCHAR(255),
    production_countries VARCHAR(255),
    release_date DATE,
    revenue FLOAT,
    runtime FLOAT,
    spoken_languages VARCHAR(255),
    status VARCHAR(100),
    vote_average FLOAT,
    vote_count FLOAT
);

"""

#%%
cur.execute(user_table)
conn.commit()
# %%
cur.execute(movie_table)
conn.commit()
#%%
cur.execute(rating_table)
conn.commit()

#%%
cur.execute(watch_table)
conn.commit()

# %%

cur.close()
conn.close()
# %%
