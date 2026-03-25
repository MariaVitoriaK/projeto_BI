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

# ------------------------
# EXTRACT
# ------------------------
def extract():
    movies = pd.read_csv("movies.csv")
    ratings = pd.read_csv("ratings.csv")
    return movies, ratings

# ------------------------
# TRANSFORM
# ------------------------
def transform(movies, ratings):

    # -------- LIMPEZA --------
    movies = movies.fillna("Desconhecido")
    ratings = ratings.dropna()

    # -------- GÊNEROS --------
    movies["genero"] = movies["genres"].str.split("|")
    movies = movies.explode("genero")

    # -------- DATAS --------
    ratings["data"] = pd.to_datetime(ratings["timestamp"], unit="s")
    ratings["dia"] = ratings["data"].dt.day
    ratings["mes"] = ratings["data"].dt.month
    ratings["ano"] = ratings["data"].dt.year
    ratings["trimestre"] = ratings["data"].dt.quarter

    return movies, ratings

# ------------------------
# LOAD
# ------------------------
def load(movies, ratings):
    conn = connect()
    cur = conn.cursor()

    # -------- DIM_FILME --------
    filmes_unicos = movies[["movieId", "title", "genero"]].drop_duplicates()

    cur.executemany(
        """
        INSERT INTO dim_filme (movie_id, titulo, genero)
        VALUES (%s, %s, %s)
        ON CONFLICT (movie_id, genero) DO NOTHING
        """,
        [(int(row.movieId), row.title, row.genero) for _, row in filmes_unicos.iterrows()]
    )

    # -------- DIM_USUARIO --------
    usuarios = ratings["userId"].drop_duplicates()

    cur.executemany(
        """
        INSERT INTO dim_usuario (user_id)
        VALUES (%s)
        ON CONFLICT (user_id) DO NOTHING
        """,
        [(int(u),) for u in usuarios]
    )

    # -------- DIM_DATA --------
    datas = ratings[["data","dia","mes","ano","trimestre"]].drop_duplicates()

    cur.executemany(
        """
        INSERT INTO dim_data (data, dia, mes, ano, trimestre)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (data) DO NOTHING
        """,
        [
            (row.data.date(), int(row.dia), int(row.mes),
             int(row.ano), int(row.trimestre))
            for _, row in datas.iterrows()
        ]
    )

    conn.commit()

    # -------- CRIAR MAPAS --------
    cur.execute("SELECT id_data, data FROM dim_data")
    map_data = {row[1]: row[0] for row in cur.fetchall()}

    cur.execute("SELECT id_usuario, user_id FROM dim_usuario")
    map_usuario = {row[1]: row[0] for row in cur.fetchall()}

    cur.execute("SELECT id_filme, movie_id FROM dim_filme")
    map_filme = {}
    for row in cur.fetchall():
        map_filme[row[1]] = row[0]

    # -------- FATO_AVALIACAO --------
    fato = []

    for _, r in ratings.iterrows():
        id_data = map_data.get(r.data.date())
        id_usuario = map_usuario.get(int(r.userId))
        id_filme = map_filme.get(int(r.movieId))

        if id_data and id_usuario and id_filme:
            fato.append((id_data, id_usuario, id_filme, float(r.rating)))

    cur.executemany(
        """
        INSERT INTO fato_avaliacao (id_data, id_usuario, id_filme, rating)
        VALUES (%s, %s, %s, %s)
        """,
        fato
    )

    conn.commit()
    cur.close()
    conn.close()

# ------------------------
# MAIN
# ------------------------
def main():
    movies, ratings = extract()
    movies, ratings = transform(movies, ratings)
    load(movies, ratings)
    print("✨ ETL concluído com sucesso.")

if __name__ == "__main__":
    main()