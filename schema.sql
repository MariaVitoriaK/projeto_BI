
CREATE TABLE dim_data (
    id_data SERIAL PRIMARY KEY,
    data DATE,
    dia INT,
    mes INT,
    ano INT,
    trimestre INT
);

CREATE TABLE dim_usuario (
    id_usuario SERIAL PRIMARY KEY,
    user_id INT
);

CREATE TABLE dim_filme (
    id_filme SERIAL PRIMARY KEY,
    movie_id INT,
    titulo TEXT,
    genero TEXT
);

CREATE TABLE fato_avaliacao (
    id SERIAL PRIMARY KEY,
    id_data INT REFERENCES dim_data(id_data),
    id_usuario INT REFERENCES dim_usuario(id_usuario),
    id_filme INT REFERENCES dim_filme(id_filme),
    rating FLOAT
);
