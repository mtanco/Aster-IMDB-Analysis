--name=base_data_cnt
SELECT COUNT(*) FROM mt_movie_genres; --14,092

--name=base_data_sample
SELECT * FROM mt_movie_genres LIMIT 10;
--movie_id	genre
--1052		Adventure
--1052		Crime
--1052		Drama
--1052		Western
--1381		Comedy
--1381		Crime
--1578		Adventure
--1578		Comedy
--1578		Crime
--1578		Drama

--name=genre_cnt
SELECT genre, COUNT(*) FROM mt_movie_genres GROUP BY 1 ORDER BY 2 DESC;
--genre			count(1)
--Drama			2506
--Comedy		1843
--Thriller		1368
--Action		1119
--Romance		1077
--Adventure		894
--Crime			865
--Sci-Fi		598
--Fantasy		595
--Horror		547
--Family		538
--Mystery		479
--Biography		284
--Animation		237
--Music			210
--War			199
--History		197
--Sport			179
--Musical		131
--Documentary	117
--Western		92
--Film-Noir		6
--Short			5
--News			3
--Reality-TV	2
--Game-Show		1

--name=number_of_genres_per_film_trend
SELECT CNT || ' Genres' AS genre_cnt, COUNT(*) FROM (
	SELECT movie_id, COUNT(*) AS CNT 
	FROM mt_movie_genres 
	GROUP BY 1
) AS X GROUP BY 1 ORDER BY 2 DESC;

--cnt	count(1)
--3		1582
--2		1323
--4		944
--1		618
--5		342
--6		73
--7		18
--8		4

--name=genre_relationships
SELECT 
	'[' || A.genre || ',' || B.genre || ']' AS PATH
	,COUNT(*) AS CNT
FROM mt_movie_genres AS A
JOIN mt_movie_genres AS B
	ON A.movie_id = B.movie_id
	AND A.genre <> B.genre
GROUP BY A.genre, B.genre;
