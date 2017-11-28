SELECT 
	movie_id,movie_title,title_year,facenumber_in_poster
	,duration,content_rating,language,country
	,genres,plot_keywords
	,director_name,actor_1_name,actor_2_name,actor_3_name
	,movie_facebook_likes,director_facebook_likes,actor_1_facebook_likes
	,actor_2_facebook_likes,actor_3_facebook_likes,cast_total_facebook_likes
	,num_voted_users,num_critic_for_reviews,num_user_for_reviews,gross,budget
	,imdb_score ,aspect_ratio
FROM imdb_scrape LIMIT 100

DROP TABLE mt_movies
CREATE TABLE mt_movies DISTRIBUTE BY HASH(movie_id) COMPRESS LOW AS
SELECT 
	movie_id
	,movie_title
	,title_year
	,facenumber_in_poster as faces_in_poster
	,duration	
	,content_rating
	,language
	,country
	,movie_facebook_likes
	,cast_total_facebook_likes
	,num_voted_users
	,num_critic_for_reviews
	,num_user_for_reviews
	,gross
	,budget
	,imdb_score
	,aspect_ratio
FROM imdb_scrape;

CREATE TABLE mt_movie_genres DISTRIBUTE BY HASH(movie_id) COMPRESS LOW AS
SELECT movie_id,REGEXP_SPLIT_TO_TABLE(genres,'\\|') as genre
FROM imdb_scrape;

CREATE TABLE mt_movie_plot_keywords DISTRIBUTE BY HASH(movie_id) COMPRESS LOW AS
SELECT movie_id,REGEXP_SPLIT_TO_TABLE(plot_keywords,'\\|') as genre
FROM imdb_scrape;

CREATE TABLE mt_movie_jobs DISTRIBUTE BY HASH(movie_id) COMPRESS LOW AS
SELECT movie_id, director_name, director_facebook_likes, 'Director' as job_role
FROM imdb_scrape
UNION ALL
SELECT movie_id, actor_1_name, director_facebook_likes, 'Actor 1' as job_role
FROM imdb_scrape
UNION ALL
SELECT movie_id, actor_2_name, director_facebook_likes, 'Actor 2' as job_role
FROM imdb_scrape
UNION ALL
SELECT movie_id, actor_3_name, director_facebook_likes, 'Actor 3' as job_role
FROM imdb_scrape;

