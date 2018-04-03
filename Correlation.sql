

SELECT '[' || regexp_replace(corr,':',',') || ']' as path
	,value as cnt
FROM Corr_Reduce (
	ON Corr_Map (
		ON (
			select num_critic_for_reviews,duration,director_facebook_likes,
			actor_1_facebook_likes,actor_2_facebook_likes,actor_3_facebook_likes,
			gross,num_voted_users,cast_total_facebook_likes,facenumber_in_poster
			,num_user_for_reviews,budget,title_year,imdb_score,aspect_ratio
			,movie_facebook_likes
			from imdb_scrape
		)
		columnpairs('[0:15]') 
		Key_Name ('key_name')
	)
	PARTITION BY key_name 
) where corr like '%imdb_score%';




SELECT *
FROM Corr_Reduce (
	ON Corr_Map (
		ON movie_genres
		columnpairs('[1:27]') 
		Key_Name ('key_name')
	)
	PARTITION BY key_name 
) where corr like '%gross%'
