-- ============================
-- DIMENSÃO DATA
-- ============================
-- Armazena informações temporais para análises (dia, mês, ano, etc.)
-- Muito usada para gráficos de evolução no tempo

CREATE TABLE dim_data (
    id_data SERIAL PRIMARY KEY, -- chave substituta
    data DATE,                  -- data completa
    dia INT,                    -- dia do mês
    mes INT,                    -- mês
    ano INT,                    -- ano
    trimestre INT               -- trimestre (1 a 4)
);

-- ============================
-- DIMENSÃO USUÁRIO
-- ============================
-- Representa os usuários que fazem avaliações

CREATE TABLE dim_usuario (
    id_usuario SERIAL PRIMARY KEY, -- chave interna
    user_id INT                    -- ID original do dataset
);

-- ============================
-- DIMENSÃO FILME
-- ============================
-- Armazena informações dos filmes
-- Um filme pode aparecer mais de uma vez por causa dos gêneros

CREATE TABLE dim_filme (
    id_filme SERIAL PRIMARY KEY, -- chave substituta
    movie_id INT,                -- ID original do filme
    titulo TEXT,                 -- nome do filme
    genero TEXT                  -- gênero (1 por linha após transformação)
);

-- ============================
-- TABELA FATO: AVALIAÇÕES
-- ============================
-- Tabela central do Data Warehouse
-- Contém as métricas (ratings) e ligações com dimensões

CREATE TABLE fato_avaliacao (
    id SERIAL PRIMARY KEY,       -- identificador da linha
    id_data INT REFERENCES dim_data(id_data),       -- FK para data
    id_usuario INT REFERENCES dim_usuario(id_usuario), -- FK para usuário
    id_filme INT REFERENCES dim_filme(id_filme),    -- FK para filme
    rating FLOAT                -- nota dada pelo usuário
);

-- ============================
-- CONSTRAINTS (REGRAS)
-- ============================

-- Evita duplicação de filme + gênero
-- Necessário porque um filme pode ter vários gêneros
ALTER TABLE dim_filme 
ADD CONSTRAINT unique_filme_genero UNIQUE (movie_id, genero);

-- Evita duplicação de usuários
ALTER TABLE dim_usuario 
ADD CONSTRAINT unique_usuario UNIQUE (user_id);

-- Evita duplicação de datas
ALTER TABLE dim_data 
ADD CONSTRAINT unique_data UNIQUE (data);

-- ============================
-- DIMENSÃO TAG
-- ============================
-- Armazena palavras-chave atribuídas pelos usuários
-- Ex: "funny", "romantic", "dark"

CREATE TABLE dim_tag (
    id_tag SERIAL PRIMARY KEY, -- chave substituta
    tag TEXT UNIQUE            -- nome da tag (único)
);

-- ============================
-- TABELA FATO: TAGS
-- ============================
-- Registra quais usuários atribuíram quais tags aos filmes

CREATE TABLE fato_tag (
    id_tag SERIAL PRIMARY KEY, -- ID da linha

    id_usuario INT,            -- FK usuário
    id_filme INT,              -- FK filme
    id_data INT,               -- FK data
    tag_id INT,               -- FK tag

    FOREIGN KEY (id_usuario) REFERENCES dim_usuario(id_usuario),
    FOREIGN KEY (id_filme) REFERENCES dim_filme(id_filme),
    FOREIGN KEY (id_data) REFERENCES dim_data(id_data),
    FOREIGN KEY (tag_id) REFERENCES dim_tag(id_tag)
);

-- ============================
-- LIMPEZA DAS TABELAS
-- ============================
-- Usado para resetar o banco antes de rodar o ETL novamente

-- Remove dados da tabela fato e reinicia IDs
TRUNCATE TABLE fato_avaliacao RESTART IDENTITY CASCADE;

-- Remove dados das dimensões
TRUNCATE TABLE dim_data RESTART IDENTITY CASCADE;
TRUNCATE TABLE dim_usuario RESTART IDENTITY CASCADE;
TRUNCATE TABLE dim_filme RESTART IDENTITY CASCADE;

-- OBS:
-- CASCADE remove também dados relacionados automaticamente
-- RESTART IDENTITY reinicia os IDs (volta para 1)