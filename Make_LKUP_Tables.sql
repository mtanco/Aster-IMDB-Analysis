/* Michelle Tanco
 * 2018 April 4th
 * Explore the raw data from the IMDB Kaggle Scrape
 * Make look up tables to use else where
 */

/*Columns are in a non-intuitive order, here is them grouped by similar content*/

--movie_id,movie_title,title_year,facenumber_in_poster
--duration
--content_rating,language,country
--genres,plot_keywords
--director_name,actor_1_name,actor_2_name,actor_3_name
--movie_facebook_likes,director_facebook_likes,actor_1_facebook_likes
--actor_2_facebook_likes,actor_3_facebook_likes,cast_total_facebook_likes
--num_voted_users,num_critic_for_reviews,num_user_for_reviews
--gross,budget
--imdb_score,aspect_ratio


-- Plot keywords table
drop table if exists lkup_plot_words;
create table lkup_plot_words 
	distribute by hash(movie_id) compress low as
select movie_id,
 	regexp_split_to_table(plot_keywords, '\\|') as plot_wrd
from imdb_scrape;

select * from lkup_plot_words order by 1 limit 5;
--movie_id	plot_wrd
--1			future
--1			marine
--1			avatar
--1			paraplegic
--1			native

-- Plot keywords table
drop table if exists lkup_genres;
create table lkup_genres 
	distribute by hash(movie_id) compress low as
select movie_id,
 	regexp_split_to_table(genres, '\\|') as genre
from imdb_scrape;

select * from lkup_genres order by 1 limit 5;
--movie_id	genre
--1			Sci-Fi
--1			Fantasy
--1			Action
--1			Adventure
--2			Adventure

drop table if exists lkup_genres_all;
create table lkup_genres_all 
	distribute by hash(movie_id) compress low as
select mv_gnr_all.movie_id
	,mv_gnr_all.genre
	,case 
		when lkup_genres.genre is not null 
		then 1 else 0 end as is_genre
from (
	select *
	from (
		select distinct movie_id
		from lkup_genres
		where movie_id in (1,2)
	) as mv_1
	right outer join (
		select distinct genre
		from lkup_genres
	) as lkup
	on 1 = 1 
) mv_gnr_all
left outer join lkup_genres
on mv_gnr_all.movie_id = lkup_genres.movie_id
and mv_gnr_all.genre = lkup_genres.genre;

select * from lkup_genres_all order by 1 limit 5;
--movie_id	genre		is_genre
--1			News		0
--1			Drama		0
--1			Mystery		0
--1			Adventure	1
--1			Sci-Fi		1

drop table if exists lkup_genres_pivot;
create table lkup_genres_pivot 
	distribute by hash(movie_id) compress low as
SELECT * FROM pivot (
	ON lkup_genres_all partition by movie_id order by genre
	partitions('movie_id')
	metrics('is_genre')
	pivot_column('genre')
	pivot_keys('Action','Adventure','Animation','Biography','Comedy','Crime','Documentary','Drama','Family','Fantasy','Film-Noir','Game-Show','History','Horror','Music','Musical','Mystery','News','Reality-TV','Romance','Sci-Fi','Short','Sport','Thriller','War','Western')
) ;

drop table if exists lkup_jobs;
create table lkup_jobs 
	distribute by hash(movie_id) compress low as
select movie_id, director_name as name
	, director_facebook_likes as facebook_likes, 'Director' as job_role
from imdb_scrape
union all
select movie_id, actor_1_name, actor_1_facebook_likes, 'Actor 1' as job_role
from imdb_scrape
union all
select movie_id, actor_2_name, actor_2_facebook_likes, 'Actor 2' as job_role
from imdb_scrape
union all
select movie_id, actor_3_name, actor_3_facebook_likes, 'Actor 3' as job_role
from imdb_scrape;

select * from lkup_jobs order by 1 limit 5;
--movie_id	name				facebook_likes	job_role
--1			CCH Pounder			1000			Actor 1
--1			Joel David Moore	936				Actor 2
--1			James Cameron		0				Director
--1			Wes Studi			855				Actor 3
--2			Johnny Depp			40000			Actor 1

