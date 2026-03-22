-- 1. Filmes mais bem avaliados
SELECT f.titulo, AVG(a.rating) AS media
FROM fato_avaliacao a
JOIN dim_filme f ON a.id_filme = f.id_filme
GROUP BY f.titulo
HAVING COUNT(*) > 50
ORDER BY media DESC
LIMIT 10;


-- 2. Filmes com maior número de avaliações
SELECT f.titulo, COUNT(*) AS total_avaliacoes
FROM fato_avaliacao a
JOIN dim_filme f ON a.id_filme = f.id_filme
GROUP BY f.titulo
ORDER BY total_avaliacoes DESC
LIMIT 10;


-- 3. Gêneros mais populares
SELECT f.genero, COUNT(*) AS total
FROM dim_filme f
JOIN fato_avaliacao a ON f.id_filme = a.id_filme
GROUP BY f.genero
ORDER BY total DESC;


-- 4. Média de avaliação por gênero
SELECT f.genero, AVG(a.rating) AS media
FROM dim_filme f
JOIN fato_avaliacao a ON f.id_filme = a.id_filme
GROUP BY f.genero
ORDER BY media DESC;


-- 5. Evolução das avaliações ao longo do tempo
SELECT d.ano, COUNT(*) AS total
FROM fato_avaliacao a
JOIN dim_data d ON a.id_data = d.id_data
GROUP BY d.ano
ORDER BY d.ano;


-- 6. Anos com mais avaliações
SELECT d.ano, COUNT(*) AS total
FROM fato_avaliacao a
JOIN dim_data d ON a.id_data = d.id_data
GROUP BY d.ano
ORDER BY total DESC;


-- 7. Relação entre popularidade e média de avaliação
SELECT f.titulo,
       COUNT(*) AS total_avaliacoes,
       AVG(a.rating) AS media
FROM fato_avaliacao a
JOIN dim_filme f ON a.id_filme = f.id_filme
GROUP BY f.titulo
HAVING COUNT(*) > 50
ORDER BY total_avaliacoes DESC;


-- 8. Filmes populares com baixa avaliação
SELECT f.titulo,
       COUNT(*) AS total_avaliacoes,
       AVG(a.rating) AS media
FROM fato_avaliacao a
JOIN dim_filme f ON a.id_filme = f.id_filme
GROUP BY f.titulo
HAVING COUNT(*) > 50 AND AVG(a.rating) < 3
ORDER BY total_avaliacoes DESC;


-- 9. Usuários mais ativos
SELECT u.user_id, COUNT(*) AS total_avaliacoes
FROM fato_avaliacao a
JOIN dim_usuario u ON a.id_usuario = u.id_usuario
GROUP BY u.user_id
ORDER BY total_avaliacoes DESC
LIMIT 10;


-- 10. Distribuição das notas
SELECT rating, COUNT(*) AS total
FROM fato_avaliacao
GROUP BY rating
ORDER BY rating;
