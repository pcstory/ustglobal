from ast import literal_eval

import pandas as pd
from google.cloud import storage


# TODO - Conver the pandas DataFrame to PySpark DataFrame

def process():
    print('ELT... ')
    bucket_name = 'ust-data'
    source_blob_name = 'raw/movies_metadata.csv'
    destination_file_name = 'movies_metadata.csv'
    # DOWNLOAD
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print('Blob {} downloaded to {}.'.format(source_blob_name, destination_file_name))

    header_line = ''
    with open("movies_metadata.csv", "r") as readfile:
        header_line = readfile.readline()

    print('Prep Data...')
    md = pd.read_csv('movies_metadata.csv', error_bad_lines=False)

    # Drop not strat with True False
    valid_true_false = ['False', 'True']
    md = md[md['adult'].isin(valid_true_false)]

    md = md.drop(
        ['id', 'adult', 'belongs_to_collection', 'budget', 'homepage', 'imdb_id', 'original_language', 'overview',
         'spoken_languages', 'tagline', 'poster_path', 'production_companies', 'production_countries', 'status',
         'video'], axis=1)

    # Fill Default Date
    md['release_date'].fillna(value='1900-01-01', inplace=True)
    md['year'] = md['release_date'].str.slice(0, 4)
    md['month'] = md['release_date'].str.slice(5, 7)

    md['genres_list'] = md['genres'].fillna('').apply(literal_eval).apply(
        lambda x: [i['name'] for i in x] if isinstance(x, list) else [])
    md['genres_list'] = md['genres_list'].apply(lambda x: str(x).strip('[]'))
    md['genres_list'] = md['genres_list'].apply(lambda x: str(x).replace('\'', ''))

    md = md.drop(['genres'], axis=1)

    c_year = pd.DataFrame(columns=['id', 'year'])
    c_year['year'] = md['year'].unique()
    c_year = c_year.sort_values(['year'])
    c_year['id'] = c_year.reset_index(inplace=False).index
    c_year.to_csv('year.csv', index=False)
    c_year.to_csv('d_year.csv', index=False)
    bucket.blob('processed/d_year.csv').upload_from_filename('d_year.csv')

    c_month = pd.DataFrame(columns=['id', 'month'])
    c_month['month'] = md['month'].unique()
    c_month = c_month.sort_values(['month'])
    c_month['id'] = c_month.reset_index(inplace=False).index
    c_month.to_csv('d_month.csv', index=False)
    bucket.blob('processed/d_month.csv').upload_from_filename('d_month.csv')

    md_fact = pd.merge(md, c_year, on='year', how='left')
    md_fact = pd.merge(md_fact, c_month, on='month', how='left')
    md_fact = md_fact.drop(['year', 'month'], axis=1)
    md_fact.rename(columns={"id_y": "year_id"}, inplace=True)
    md_fact.rename(columns={"id": "month_id"}, inplace=True)
    md_fact.to_csv('f_movie.csv', index=False)
    bucket.blob('processed/f_movie.csv').upload_from_filename('f_movie.csv')
    print('Completed!')

if __name__ == '__main__':
    process()
