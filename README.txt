
Projeto BI - Data Warehouse MovieLens

Arquivos:
schema.sql -> cria as tabelas do Data Warehouse
etl_movielens_dw.py -> script Python ETL
queries.sql -> consultas analíticas

Passos:

1. Criar banco PostgreSQL
CREATE DATABASE dw_filmes;

2. Executar schema.sql

3. Colocar dataset:
movies.csv
ratings.csv

4. Instalar bibliotecas:
pip install pandas psycopg2-binary

5. Rodar:
python etl_movielens_dw.py
