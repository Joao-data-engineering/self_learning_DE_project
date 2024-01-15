table_info: dict = {
    'name_basics': ['/opt/storage/cache/name_basics.tsv.gz', 'https://datasets.imdbws.com/name.basics.tsv.gz'],
    'title_akas': ['/opt/storage/cache/title_akas.tsv.gz', 'https://datasets.imdbws.com/title.akas.tsv.gz'],
    'title_basics': ['/opt/storage/cache/title_basics.tsv.gz', 'https://datasets.imdbws.com/title.basics.tsv.gz'],
    'title_crew': ['/opt/storage/cache/title_crew.tsv.gz', 'https://datasets.imdbws.com/title.crew.tsv.gz'],
    'title_episode': ['/opt/storage/cache/title_episode.tsv.gz', 'https://datasets.imdbws.com/title.episode.tsv.gz'],
    'title_principals': ['/opt/storage/cache/title_principals.tsv.gz', 'https://datasets.imdbws.com/title.principals.tsv.gz'],
    'title_ratings': ['/opt/storage/cache/title_ratings.tsv.gz', 'https://datasets.imdbws.com/title.ratings.tsv.gz']
}

tables_partition_key : dict = {
    'name_basics': 'birthYear',
    'title_akas': None,
    'title_basics': 'startYear',
    'title_crew': None,
    'title_episode': None,
    'title_principals': 'category',
    'title_ratings': None 
}

tables_primary_key : dict = {
    'name_basics': 'nconst',
    'title_akas': ['titleId','ordering'],
    'title_basics': 'tconst',
    'title_crew': 'tconst',
    'title_episode': 'tconst',
    'title_principals': ['tconst','nconst'],
    'title_ratings': 'tconst' 
}
