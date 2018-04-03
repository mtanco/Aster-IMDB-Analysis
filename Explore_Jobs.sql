select '[' || role_1 || ',' || role_2 || ']' as path
	,count(*)/sum(count(*)) over (partition by role_1) as cnt
from nPath(
	on imdb_name_role partition by name order by title_year
	mode(overlapping)
	symbols(true as A)
	pattern('A.A')
	result(
		first(role of A) as role_1
		,last(role of A) as role_2
		
	)
) group by 1,2
order by 1,2;

drop table if exists actor_graph;
create table actor_graph 
	distribute by hash(actor_1_name) compress low as
select actor_1_name, actor_2_name, sum(cnt) as cnt
from (
	select actor_1_name, actor_2_name,count(*) as cnt
	from imdb_scrape
	group by 1,2
	
	union all
	
	select actor_2_name, actor_3_name,count(*) as cnt
	from imdb_scrape
	group by 1,2
	
	union all
	
	select actor_3_name, actor_1_name,count(*) as cnt
	from imdb_scrape
	group by 1,2
)as x group by 1,2;
analyze actor_graph;


create table far_seperation distribute by hash(source) as
select *
from AllPairsShortestPath(
ON (select distinct actor_1_name from actor_graph) AS vertices PARTITION BY actor_1_name
ON actor_graph AS edges PARTITION BY actor_1_name
TARGETKEY('actor_2_name')
EDGEWEIGHT('cnt')
MAXDISTANCE('100')
) where distance > 6;

select count(distinct source),count(distinct target), max(distance)
from far_seperation;


select source, count(distinct target), avg(distance)
from far_seperation
group by 1
order by 2 desc
limit 100;

select count(distinct actor_1_name) from actor_graph;

drop table if exists imdb_name_role;
create table imdb_name_role distribute by hash(name) compress low as
select movie_id, 'director' as role, director_name as name, title_year
from imdb_scrape
where director_name is not null
union all
select movie_id, 'actor 1', actor_1_name, title_year
from imdb_scrape
where actor_1_name is not null
union all
select movie_id, 'actor 2', actor_2_name, title_year
from imdb_scrape
where actor_2_name is not null
union all
select movie_id, 'actor 3', actor_3_name, title_year
from imdb_scrape
where actor_3_name is not null;

--more than 1 movie in a year??
select name, title_year, count(distinct role)
from imdb_name_role
group by 1,2 having count(distinct role) > 1;

--Not accurate due to ordering
select *
from nPath(
	on imdb_name_role partition by name order by title_year
	mode(nonoverlapping)
	symbols(true as A)
	pattern('A*')
	result(
		first(name of A) as name
		,accumulate(role of A) as role_path
	)
)
