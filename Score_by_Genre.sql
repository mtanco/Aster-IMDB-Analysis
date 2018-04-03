drop table if exists movie_genres;
create table movie_genres distribute by hash(movie_id) compress low as 
SELECT movie_id,imdb_score
	,coalesce("value_Action",'N') as Action
	,coalesce("value_Adventure",'N') as Adventure
	,coalesce("value_Animation",'N') as Animation
	,coalesce("value_Biography",'N') as Biography
	,coalesce("value_Comedy",'N') as Comedy
	,coalesce("value_Crime",'N') as Crime
	,coalesce("value_Documentary",'N') as Documentary
	,coalesce("value_Drama",'N') as Drama
	,coalesce("value_Family",'N') as Family
	,coalesce("value_Fantasy",'N') as fantasy
	,coalesce("value_Film-Noir",'N') as filmnoir
	,coalesce("value_Game-Show",'N') as gameshow
	,coalesce("value_History",'N') as history
	,coalesce("value_Horror",'N') as horror
	,coalesce("value_Music",'N') as music
	,coalesce("value_Musical",'N') as musical
	,coalesce("value_Mystery",'N') as mystery
	,coalesce("value_News",'N') as news
	,coalesce("value_Reality-TV",'N') as Realitytv
	,coalesce("value_Romance",'N') as Romance
	,coalesce("value_Sci-Fi",'N') as Scifi
	,coalesce("value_Short",'N') as Short
	,coalesce("value_Sport",'N') as Sport
	,coalesce("value_Thriller",'N') as Thriller
	,coalesce("value_War",'N') as War
	,coalesce("value_Western",'N') as Western
FROM pivot(
	ON (
		select movie_id, imdb_score, 'Y' as value
			, regexp_split_to_table(genres, '\\|') as genre
		from imdb_scrape
	) PARTITION BY movie_id,imdb_score
	PARTITIONS('movie_id','imdb_score')
	PIVOT_KEYS('Sci-Fi','Drama','Mystery','News','Game-Show'
		,'Adventure','Documentary','Action','Musical','Film-Noir'
		,'Music','Reality-TV','Biography','Comedy','Romance','Crime'
		,'Family','Short','Sport','Western','History','Horror','War'
		,'Animation','Fantasy','Thriller')
	PIVOT_COLUMN('genre')
	METRICS('value')
);
drop table if exists movie_genres_train;
create table movie_genres_train distribute by hash(movie_id) as
select * from sample(
	on movie_genres SAMPLEFRACTION('0.8') seed('1'));
drop table if exists movie_genres_test;
create table movie_genres_test distribute by hash(movie_id)  as
select * from movie_genres where movie_id not in 
(select movie_id from movie_genres_train);
drop table if exists movie_genres_rf_model;
SELECT * FROM Forest_Drive(
	ON (SELECT 1)
	PARTITION BY 1
	INPUTTABLE('movie_genres_train')
	OUTPUTTABLE('movie_genres_rf_model')
	RESPONSE('imdb_score')
	--NUMERICINPUTS('num_voted_users')
	CATEGORICALINPUTS('SciFi','Drama','Mystery'
			,'Adventure','Action','Family'
			,'Comedy','Romance','Crime'
			,'Horror','Fantasy','Thriller')
	--[TREETYPE(tree_type)]
	--[NUMTREES(number_of_trees)]
	--[TREESIZE(tree_size)]
	--[MINNODESIZE(min_node_size)]
	--[VARIANCE(variance)]
	--[MAXDEPTH(max_depth)]
	--[NUMSURROGATES(num_surrogates)]
	--[MONITORTABLE('monitor_table_name')]
	--[DROPMONITORTABLE('true'|'false')]
);
drop table if exists movie_genres_rf_predictions;
create table movie_genres_rf_predictions distribute by hash(movie_id) as
SELECT rf.*,m.imdb_score FROM Forest_Predict(
	ON movie_genres_test
	FOREST('movie_genres_rf_model')
	--NUMERICINPUTS('num_voted_users')
	CATEGORICALINPUTS('SciFi','Drama','Mystery'
		,'Adventure','Action','Family'
		,'Comedy','Romance','Crime'
		,'Horror','Fantasy','Thriller')
	IDCOL('movie_id')
) as rf
join movie_genres_test as m on rf.movie_id = m.movie_id;
select sum( (prediction::numeric - imdb_score)^2 )/count(*) as MSE
	,sqrt(sum( (prediction::numeric - imdb_score)^2 )/count(*)) as RMSE
from movie_genres_rf_predictions;
