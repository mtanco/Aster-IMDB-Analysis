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
order by 1,2
