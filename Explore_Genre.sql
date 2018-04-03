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

--Input dataset
select * from imdb_name_role limit 10;

/*Movie ID, Role, Name, Title Year*/
--571,director,Adam McKay,2006
--1756,director,Adam McKay,2015
--660,director,Adam McKay,2008
--729,director,Adam Shankman,2005

select count(*) from imdb_name_role; --27449

/*Get career paths and count how often each occurs*/
create table career_path distribute by hash(path) compress low as
select path, count(*) as cnt
from nPath(
	on imdb_name_role partition by name order by title_year
	mode(nonoverlapping) 
	symbols(true as A)
	pattern('^A{1,5}') --first up to 5 jobs
	result(
		first(name of A) as name
		,accumulate(role of A) as path --career path
	)
)group by 1;

/*Turn the path table into json*/
INSERT INTO app_center_visualizations  (json) 
SELECT json FROM Visualizer (
    ON career_path PARTITION BY 1 
    AsterFunction('npath') 
    Title('Paths of Jobs') 
    VizType('sankey')
);

drop table if exists movie_plot;
create table movie_plot distribute by hash(docid) as
select title_year || '_' || genre as docid
	,plot as term
from (
	select title_year 
		,regexp_split_to_table(plot_keywords, '\\|') as plot
		,regexp_split_to_table(genres, '\\|') as genre
	from imdb_scrape
	order by 1
) as x;
analyze movie_plot;


select term, count(*) as cnt
from(
	select title_year ,regexp_split_to_table(plot_keywords, '\\|') as term
	from imdb_scrape
) as x group by 1
order by 2 desc limit 100


