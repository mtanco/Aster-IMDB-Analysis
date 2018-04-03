-- Movie ID, Director Name, Actor
create table director_actor_graph distribute by replication as
select director_name, actor_1_name from imdb_scrape
union
select director_name, actor_2_name from imdb_scrape
union
select director_name, actor_3_name from imdb_scrape;

-- Create table with individual actor entries
select movie_id,
	movie_title,
	country,
	title_year,
	content_rating,
	imdb_score,
	aspect_ratio,
	director_name,
	duration,
	budget,
	gross,
	plot_keywords,
	genres,
	actor_1_name,
	actor_1_facebook_likes
from imdb_scrape
union
select movie_id,
	movie_title,
	country,
	title_year,
	content_rating,
	imdb_score,
	aspect_ratio,
	director_name,
	duration,
	budget,
	gross,
	plot_keywords,
	genres,
	actor_2_name,
	actor_2_facebook_likes
from imdb_scrape
union
select movie_id,
	movie_title,
	country,
	title_year,
	content_rating,
	imdb_score,
	aspect_ratio,
	director_name,
	duration,
	budget,
	gross,
	plot_keywords,
	genres,
	actor_3_name,
	actor_3_facebook_likes
from imdb_scrape
compress low;

-- Plot keywords table
select movie_id,
	movie_name,
	director_name,
 	regexp_split_to_table(plot_keywords, '|')
from imdb_scrape;

-- Director, plot keywords
-- pSALSA


-- Plot keywords vs. IMDB score
-- Plot keywords vs. Budget:Gross ratio

-- Director gross over time
select * from nPath(
	on imdb_scrape
	partition by director_name
	order by title
	symbols()
	pattern()
)
