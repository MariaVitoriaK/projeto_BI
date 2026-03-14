
import pandas as pd
import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "database": "dw_filmes",
    "user": "postgres",
    "password": "Melissak12."
}

def connect():
    return psycopg2.connect(**DB_CONFIG)

def extract():
    movies = pd.read_csv("movies.csv")
    ratings = pd.read_csv("ratings.csv")
    return movies, ratings

def transform(movies, ratings):
    ratings["data"] = pd.to_datetime(ratings["timestamp"], unit="s")
    ratings["dia"] = ratings["data"].dt.day
    ratings["mes"] = ratings["data"].dt.month
    ratings["ano"] = ratings["data"].dt.year
    ratings["trimestre"] = ratings["data"].dt.quarter
    return movies, ratings

def load(movies, ratings):
    conn = connect()
    cur = conn.cursor()

    # inserir filmes
    for _, row in movies.iterrows():
        cur.execute(
            "INSERT INTO dim_filme (movie_id, titulo, genero) VALUES (%s,%s,%s)",
            (int(row.movieId), row.title, row.genres)
        )

    # inserir usuários
    users = ratings["userId"].unique()
    for u in users:
        cur.execute("INSERT INTO dim_usuario (user_id) VALUES (%s)", (int(u),))

    # inserir datas
    datas = ratings[["data","dia","mes","ano","trimestre"]].drop_duplicates()

    for _, row in datas.iterrows():
        cur.execute(
            "INSERT INTO dim_data (data,dia,mes,ano,trimestre) VALUES (%s,%s,%s,%s,%s)",
            (row.data, int(row.dia), int(row.mes), int(row.ano), int(row.trimestre))
        )

    conn.commit()

    # inserir avaliações na tabela fato
    for _, r in ratings.iterrows():
        cur.execute(
            """
            INSERT INTO fato_avaliacao (id_data,id_usuario,id_filme,rating)
            VALUES (
                (SELECT id_data FROM dim_data WHERE data=%s LIMIT 1),
                (SELECT id_usuario FROM dim_usuario WHERE user_id=%s LIMIT 1),
                (SELECT id_filme FROM dim_filme WHERE movie_id=%s LIMIT 1),
                %s
            )
            """,
            (r.data.date(), int(r.userId), int(r.movieId), float(r.rating))
        )

    conn.commit()
    cur.close()
    conn.close()

def main():
    movies, ratings = extract()
    movies, ratings = transform(movies, ratings)
    load(movies, ratings)
    print("ETL concluído com sucesso.")

if __name__ == "__main__":
    main()
