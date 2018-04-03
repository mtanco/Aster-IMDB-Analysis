--max of 8
create table genre_pairs distribute by hash(movie_id) as
select * from npath(
	on (
		select movie_id, imdb_score, regexp_split_to_table(genres, '\\|') as genre
		from imdb_scrape
	) partition by movie_id order by genre
	mode(overlapping)
	symbols(true as A, true as B)
	pattern('A.A')
	result(
		first(movie_id of A) as movie_id
		,first(imdb_score of A) as score
		,accumulate(genre of A) as pair
	)
) 
union all 
select * from npath(
	on (
		select movie_id, imdb_score, regexp_split_to_table(genres, '\\|') as genre
		from imdb_scrape
	) partition by movie_id order by genre
	mode(overlapping)
	symbols(true as A, true as B)
	pattern('A.B.A')
	result(
		first(movie_id of A) as movie_id
		,first(imdb_score of A) as score
		,accumulate(genre of A) as pair
	)
) 

union all 

select * from npath(
	on (
		select movie_id, imdb_score, regexp_split_to_table(genres, '\\|') as genre
		from imdb_scrape
	) partition by movie_id order by genre
	mode(overlapping)
	symbols(true as A, true as B)
	pattern('A.B{2}.A')
	result(
		first(movie_id of A) as movie_id
		,first(imdb_score of A) as score
		,accumulate(genre of A) as pair
	)
) 
union all

select * from npath(
	on (
		select movie_id, imdb_score, regexp_split_to_table(genres, '\\|') as genre
		from imdb_scrape
	) partition by movie_id order by genre
	mode(overlapping)
	symbols(true as A, true as B)
	pattern('A.B{3}.A')
	result(
		first(movie_id of A) as movie_id
		,first(imdb_score of A) as score
		,accumulate(genre of A) as pair
	)
) 
union all
select * from npath(
	on (
		select movie_id, imdb_score, regexp_split_to_table(genres, '\\|') as genre
		from imdb_scrape
	) partition by movie_id order by genre
	mode(overlapping)
	symbols(true as A, true as B)
	pattern('A.B{4}.A')
	result(
		first(movie_id of A) as movie_id
		,first(imdb_score of A) as score
		,accumulate(genre of A) as pair
	)
) 

union all select * from npath(
	on (
		select movie_id, imdb_score, regexp_split_to_table(genres, '\\|') as genre
		from imdb_scrape
	) partition by movie_id order by genre
	mode(overlapping)
	symbols(true as A, true as B)
	pattern('A.B{5}.A')
	result(
		first(movie_id of A) as movie_id
		,first(imdb_score of A) as score
		,accumulate(genre of A) as pair
	)
) 
union all

select * from npath(
	on (
		select movie_id, imdb_score, regexp_split_to_table(genres, '\\|') as genre
		from imdb_scrape
	) partition by movie_id order by genre
	mode(overlapping)
	symbols(true as A, true as B)
	pattern('A.B{6}.A')
	result(
		first(movie_id of A) as movie_id
		,first(imdb_score of A) as score
		,accumulate(genre of A) as pair
	)
) 
;

select pair as path, avg(score) as cnt
from genre_pairs
group by 1
