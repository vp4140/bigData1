import pandas as pd

title_basicsDF = pd.read_table('title.basics.tsv')
# df = pd.read_table('title.basics.tsv')
# Removing adult

title_basicsDF = title_basicsDF[title_basicsDF['isAdult'] == 0]
# reducing the data

# slicing the tconst so that it can be made into a integer later
title_basicsDF.tconst = title_basicsDF.tconst.str.slice(start=2)

# changing data type
title_basicsDF['tconst'] = title_basicsDF["tconst"].map(int)

genre_enum_df = title_basicsDF[['tconst', 'genres']].copy()
# Exploding different generes so that it can be stored as a enum in the genre_enum table
genre_enum_df.genres = title_basicsDF.genres.str.split(',')
genre_enum_df = genre_enum_df.explode('genres')

data = {
    "genre_name": genre_enum_df.genres.unique()
}
# Creating the final genre enum file
final_genre_enumdf = pd.DataFrame(data)
# Cleaning the file
final_genre_enumdf = final_genre_enumdf.replace('\\N', 'No_Genre')
# getting unique index
final_genre_enumdf['genre_number'] = final_genre_enumdf.index

# Performing join to create the genre_map
genre_mapper = (genre_enum_df.merge(final_genre_enumdf, left_on='genres', right_on='genre_name')
                .reindex(columns=['tconst', 'genre_number']))

# Saving the file to the title_data csv file
title_basicsDF.to_csv(r'/Users/vishalpanchidi/Desktop/Big_data_assignments/Assigment1_filtered/title_data.csv',
                      index=False, header=True)
# Saving the file to the genre_mapper csv file
genre_mapper.to_csv(r'/Users/vishalpanchidi/Desktop/Big_data_assignments/Assigment1_filtered/genre_mapper.csv',
                    index=False, header=True)

# Saving the file to the final_genre_enumdf csv file
final_genre_enumdf.to_csv(r'/Users/vishalpanchidi/Desktop/Big_data_assignments/Assigment1_filtered/genre_enumdf.csv',
                          index=False, header=True)

# reading the principal data
titleMapperdf = pd.read_table('title.principals.tsv')
titleMapperdf.category = titleMapperdf.category.str.split(',')
titleMapperdf = titleMapperdf.explode('category')
titleMapperdf = titleMapperdf[(titleMapperdf['category'] == 'director') | (titleMapperdf['category'] == 'producer') | (
        titleMapperdf['category'] == 'actor') | (titleMapperdf['category'] == 'actress')]
titleMapperdf = titleMapperdf[['tconst', 'nconst', 'category']]
titleMapperdf.nconst = titleMapperdf.nconst.str.slice(start=2).astype(int)

titleCrewdf = pd.read_table('title.crew.tsv')
# divding the directors and writers from the crew data

# Director crew data processing
titleCrewdfDirector = titleCrewdf[['tconst', 'directors']]
titleCrewdfDirector.directors = titleCrewdfDirector.directors.str.split(',')
titleCrewdfDirector = titleCrewdfDirector.explode('directors')
titleCrewdfDirector.tconst = titleCrewdfDirector.tconst.str.slice(start=2).astype(int)
titleCrewdfDirector.directors = titleCrewdfDirector.directors.str.slice(start=2)
titleCrewdfDirector = titleCrewdfDirector[titleCrewdfDirector['directors'] != ""]
titleCrewdfDirector['directors'] = titleCrewdfDirector['directors'].map(int)

# Creating the column category for director
titleCrewdfDirector['category'] = "director"

# Writer crew data processing
titleCrewdfWriter = titleCrewdf[['tconst', 'writers']]
titleCrewdfWriter.writers = titleCrewdfWriter.writers.str.split(',')
titleCrewdfWriter = titleCrewdfWriter.explode('writers')
titleCrewdfWriter = titleCrewdfWriter[titleCrewdfWriter['writers'] != "\\N"]
titleCrewdfWriter.tconst = titleCrewdfWriter.tconst.str.slice(start=2).astype(int)
titleCrewdfWriter.writers = titleCrewdfWriter.writers.str.slice(start=2).astype(int)
titleCrewdfWriter['category'] = "writer"

# Changing the column names to make the union possible
titleCrewdfWriter.rename(columns={'writers': 'nconst'}, inplace=True)
titleCrewdfDirector.rename(columns={'directors': 'nconst'}, inplace=True)

# union operation between director and writer data
resultTitleMapper = (pd.concat([titleCrewdfWriter, titleCrewdfDirector]))

# union between writer, director and all the data from principal dataset
resultTitleMapper = (pd.concat([resultTitleMapper, titleMapperdf]))

#saving the data in CSV
resultTitleMapper.to_csv(
    r'/Users/vishalpanchidi/Desktop/Big_data_assignments/Assigment1_filtered/resultTitleMapper.csv', index=False,
    header=True)

# reading the names
names = pd.read_table('name.basics.tsv')
names.primaryProfession = names.primaryProfession.str.split(',')
names = names.explode('primaryProfession')
# Including the data only with limited professions
names = names[(names['primaryProfession'] == 'director') | (names['primaryProfession'] == 'producer') | (
        names['primaryProfession'] == 'actor') | (names['primaryProfession'] == 'actress') | (
                      names['primaryProfession'] == 'writer')]
names = names[['nconst', 'primaryName', 'birthYear', 'deathYear']]
names.nconst = names.nconst.str.slice(start=2)
names.nconst = names.nconst.astype(int)

names['birthYear'] = names['birthYear'].replace('\\N', '0000')
names['deathYear'] = names['deathYear'].replace('\\N', '0000')
names = names.drop_duplicates(subset="nconst")

names.to_csv(r'/Users/vishalpanchidi/Desktop/Big_data_assignments/Assigment1_filtered/names.csv', index=False,
             header=True)

# reading title name mapper for further processing
name_title_mapper = pd.read_csv('resultTitleMapper.csv')
# dropping duplicated
name_title_mapper = name_title_mapper.drop_duplicates()

# reading  name csv data for further processing
names_data = pd.read_csv('names.csv')


name_title_mapper = names_data.join(name_title_mapper.set_index("nconst"), on="nconst", how="inner")
name_title_mapper = name_title_mapper[['tconst', 'nconst', 'category']]

title_data_csv = pd.read_csv('title_data.csv')
name_title_mapper = title_data_csv.join(name_title_mapper.set_index("tconst"), on="tconst", how="inner")
name_title_mapper = name_title_mapper[['tconst', 'nconst', 'category']]
name_title_mapper.to_csv(
    r'/Users/vishalpanchidi/Desktop/Big_data_assignments/Assigment1_filtered/resultTitleMapper.csv', index=False,
    header=True)

# IMDB=# \copy genre_enum( genre_number, genre_name) from '/Users/vishalpanchidi/Desktop/Big_data_assignments/Assigment1_filtered/genre_enumdf.csv' (format csv, delimiter ',', header true);
# IMDB=# \copy name_data(nconst,primaryName,birthYear,deathYear) from '/Users/vishalpanchidi/Desktop/Big_data_assignments/Assigment1_filtered/names.csv' (format csv, delimiter ',', header true);
# IMDB=# \copy resultTitleMapper( tconst,nconst,category) from '/Users/vishalpanchidi/Desktop/Big_data_assignments/Assigment1_filtered/resultTitleMapper.csv' (format csv, delimiter ',', header true);
# IMDB=# \copy ratings( tconst, averageRating, numVotes) from '/Users/vishalpanchidi/Desktop/Big_data_assignments/Assigment1_filtered/ratings_of_titles.csv' (format csv, delimiter ',', header true);
