
SELECT f.titulo, AVG(a.rating) AS media
FROM fato_avaliacao a
JOIN dim_filme f ON a.id_filme = f.id_filme
GROUP BY f.titulo
ORDER BY media DESC
LIMIT 10;

SELECT f.titulo, COUNT(*) AS total_avaliacoes
FROM fato_avaliacao a
JOIN dim_filme f ON a.id_filme = f.id_filme
GROUP BY f.titulo
ORDER BY total_avaliacoes DESC
LIMIT 10;

SELECT d.ano, COUNT(*) AS total
FROM fato_avaliacao a
JOIN dim_data d ON a.id_data = d.id_data
GROUP BY d.ano
ORDER BY d.ano;
