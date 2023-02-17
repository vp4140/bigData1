Create Table title(
tconst integer,
primaryTitle varchar(500),
	originalTitle varchar(500), 
	startYear integer,
	runtimeMinutes integer,
	primary key(tconst) 
)
Create Table genre_enum(
	genre_number integer,
	genre_name varchar(500),
	primary key(genre_number) 
)
Create Table genre_mapper(
	tconst integer,
	genre_number integer,
	foreign key(tconst)references title(tconst),
	foreign key(genre_number)references genre_enum(genre_number)
)
Create Table resultTitleMapper(
	tconst integer,
	nconst integer,
	category varchar(50),
	foreign key(tconst)references title(tconst),
	foreign key(nconst)references name_data(nconst),
	primary key(tconst,nconst,category)
)
Create Table name_data(
	nconst integer,
	primaryName varchar(500),
	birthYear varchar(4),
	deathYear varchar(4),
	primary key(nconst)
)
Create Table ratings(
	tconst integer,
	averageRating integer,
	numVotes integer,
	foreign key(tconst)references title(tconst)
)
-- After python code execution
SELECT * FROM name_data where name_data.nconst = 99999999;
