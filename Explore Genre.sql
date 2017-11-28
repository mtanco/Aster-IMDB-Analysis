--name=base_data_cnt
SELECT 'All Rows' AS x, COUNT(*) AS y FROM mt_movie_genres; --14,092

--name=genre_cnt
SELECT genre, COUNT(*) AS CNT FROM mt_movie_genres GROUP BY 1 ORDER BY 2 DESC;


SELECT 
	'[' || A.genre || ',' || B.genre || ']' AS PATH
	,COUNT(*) AS CNT
FROM mt_movie_genres AS A
JOIN mt_movie_genres AS B
	ON A.movie_id = B.movie_id
	AND A.genre <> B.genre
GROUP BY A.genre, B.genre;

