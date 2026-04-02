import pandas as pd
import psycopg2

# Configuração de conexão com o banco PostgreSQL
DB_CONFIG = {
    "host": "localhost",
    "database": "dw_filmes",
    "user": "postgres",
    "password": "Melissak12."
}

# Função que cria a conexão com o banco
def connect():
    return psycopg2.connect(**DB_CONFIG)

# ------------------------
# EXTRACT
# ------------------------
def extract():
    # Leitura dos arquivos CSV (dados brutos)
    movies = pd.read_csv("movies.csv")
    ratings = pd.read_csv("ratings.csv")
    tags = pd.read_csv("tags.csv")

    # Retorna os dados para a próxima etapa
    return movies, ratings, tags

# ------------------------
# TRANSFORM
# ------------------------
def transform(movies, ratings, tags):

    # Tratamento de valores nulos
    movies = movies.fillna("Desconhecido")
    ratings = ratings.dropna()
    tags = tags.dropna()

    # -------- TRATAMENTO DE GÊNEROS --------
    # Um filme pode ter vários gêneros (ex: Action|Comedy)
    # Aqui dividimos e transformamos em várias linhas
    movies["genero"] = movies["genres"].str.split("|")
    movies = movies.explode("genero")

    # -------- TRATAMENTO DE DATAS (ratings) --------
    # Converte timestamp para data legível
    ratings["data"] = pd.to_datetime(ratings["timestamp"], unit="s")

    # Cria atributos para análise temporal
    ratings["dia"] = ratings["data"].dt.day
    ratings["mes"] = ratings["data"].dt.month
    ratings["ano"] = ratings["data"].dt.year
    ratings["trimestre"] = ratings["data"].dt.quarter

    # -------- TRATAMENTO DE DATAS (tags) --------
    tags["data"] = pd.to_datetime(tags["timestamp"], unit="s")

    return movies, ratings, tags

# ------------------------
# LOAD
# ------------------------
def load(movies, ratings, tags):
    conn = connect()
    cur = conn.cursor()

    # -------- DIM_FILME --------
    # Remove duplicados para evitar inserir filmes repetidos
    filmes_unicos = movies[["movieId", "title", "genero"]].drop_duplicates()

    # Insere na dimensão de filmes
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

    # -------- DIM_TAG --------
    tags_unicas = tags["tag"].drop_duplicates()

    cur.executemany(
        """
        INSERT INTO dim_tag (tag)
        VALUES (%s)
        ON CONFLICT (tag) DO NOTHING
        """,
        [(t,) for t in tags_unicas]
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

    # -------- CRIAÇÃO DE MAPAS --------
    # Aqui buscamos os IDs gerados no banco para usar nas tabelas fato

    cur.execute("SELECT id_data, data FROM dim_data")
    map_data = {row[1]: row[0] for row in cur.fetchall()}

    cur.execute("SELECT id_tag, tag FROM dim_tag")
    map_tag = {row[1]: row[0] for row in cur.fetchall()}

    cur.execute("SELECT id_usuario, user_id FROM dim_usuario")
    map_usuario = {row[1]: row[0] for row in cur.fetchall()}

    cur.execute("SELECT id_filme, movie_id FROM dim_filme")
    map_filme = {row[1]: row[0] for row in cur.fetchall()}

    # -------- FATO_AVALIACAO --------
    fato = []

    for _, r in ratings.iterrows():
        # Busca os IDs correspondentes
        id_data = map_data.get(r.data.date())
        id_usuario = map_usuario.get(int(r.userId))
        id_filme = map_filme.get(int(r.movieId))

        # Só insere se todos existirem
        if id_data and id_usuario and id_filme:
            fato.append((id_data, id_usuario, id_filme, float(r.rating)))

    cur.executemany(
        """
        INSERT INTO fato_avaliacao (id_data, id_usuario, id_filme, rating)
        VALUES (%s, %s, %s, %s)
        """,
        fato
    )

    # -------- FATO_TAG --------
    fato_tags = []

    for _, t in tags.iterrows():
        id_usuario = map_usuario.get(int(t.userId))
        id_filme = map_filme.get(int(t.movieId))
        id_tag = map_tag.get(t.tag)
        id_data = map_data.get(t.data.date())

        if id_usuario and id_filme and id_tag and id_data:
            fato_tags.append((id_usuario, id_filme, id_data, id_tag))

    cur.executemany(
        """
        INSERT INTO fato_tag (id_usuario, id_filme, id_data, tag_id)
        VALUES (%s, %s, %s, %s)
        """,
        fato_tags
    )

    conn.commit()
    cur.close()
    conn.close()

# ------------------------
# MAIN
# ------------------------
def main():
    # Executa todo o pipeline ETL
    movies, ratings, tags = extract()
    movies, ratings, tags = transform(movies, ratings, tags)
    load(movies, ratings, tags)

    print("✨ ETL concluído com sucesso.")

if __name__ == "__main__":
    main()