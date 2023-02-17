import psycopg2

# Connect to your postgres DB
try:
    conn = psycopg2.connect(host="localhost", dbname="IMDB", user="postgres", password="vishal", port=5432)
    cur = conn.cursor()
    # execute 1sr statement
    cur.execute("""INSERT INTO name_data(nconst, primaryName, birthYear, deathYear) VALUES(99999999,'The_Rochester_Movie', 1997, 2022) """)

    # execute 2nd statement
    cur.execute("""INSERT INTO name_data(nconst, primaryName, birthYear, deathYear) VALUES(99999998, 'The_Rochester_Movie2',19970,2022) """)

    # execute 3rd statement
    cur.execute("""INSERT INTO name_data(nconst, primaryName, birthYear, deathYear) VALUES(99999997,'The_Rochester_Movie3', 1998,2020) """)

    # commit the transaction
    conn.commit()
    # close the database communication
    cur.close()
    # Open a cursor to perform database operations
    cursor = conn.cursor()
except psycopg2.DatabaseError as error:
    print(error)
finally:
    if conn is not None:
        conn.close()
