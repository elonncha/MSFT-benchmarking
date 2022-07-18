import os
import urllib.request
from census import Census
import pandas as pd
import urllib.request
import zipfile
import geopandas as gpd
import numpy as np
from functools import reduce

def download_CDCPlaces(years):
    """
    data earlier than 2020 is from 500 cities (CT within city limit only)
    :param years: python list - year(s) of interest, e.g. [2018, 2019, 2020, 2021]

    :return: NA
    """

    url_dic = {'2018':'https://chronicdata.cdc.gov/api/views/k25u-mg9b/rows.csv?accessType=DOWNLOAD',
               '2019':'https://chronicdata.cdc.gov/api/views/k86t-wghb/rows.csv?accessType=DOWNLOAD',
               '2020':'https://chronicdata.cdc.gov/api/views/ib3w-k9rq/rows.csv?accessType=DOWNLOAD',
               '2021':'https://chronicdata.cdc.gov/api/views/yjkw-uj5s/rows.csv?accessType=DOWNLOAD'}

    for year in years:
        url = url_dic[str(year)]
        if '{0}_health_cdcplaces.csv'.format(year) not in os.listdir('../data/raw/'):
            urllib.request.urlretrieve(url, '../data/raw/{0}_health_cdcplaces.csv'.format(year))


def download_ACS(year, api_key,
                 table,
                 state_id, county_id,
                 acs5_variable_list, acs_name_list):
    """
    :param year: year of interest, e.g. 2021
    :param api_key: census api key (prefilled)
    :param table: table type to download - 'detailed', 'subject', 'profile'
    :param state_id: state FIPS of interest
    :param county_id: county FIPS of interest
    :param acs5_variable_list: a TUPLE of variable id
    :param acs_name_list: a list of variable names (used as column names)

    :return: NONE
    """


    # retrieve data from api package
    c = Census(api_key, year=year)
    if table == 'detailed':
        df = c.acs5.state_county_tract(tuple(acs5_variable_list), state_id, '*', '*')
    if table == 'subject':
        df = c.acs5st.state_county_tract(tuple(acs5_variable_list), state_id, '*', '*')
    if table == 'profile':
        df = c.acs5dp.state_county_tract(tuple(acs5_variable_list), state_id, '*', '*')

    # store as pandas SF and create TractFIPS column
    df = pd.DataFrame.from_dict(df)
    df['TractFIPS'] = df.state + df.county + df.tract
    output = df.loc[df.county.isin(county_id), :]. \
        drop(columns=['tract']). \
        reset_index(drop=True)

    # rename columns and write as csv
    for i, code in enumerate(acs5_variable_list):
        output = output.rename(columns={code: acs_name_list[i]})

    output.to_csv('../data/raw/{0}_SES_acs.csv'.format(year), index = False)


def load_AtlantaCT_FIPS():
    df = pd.read_csv('../data/raw/2019_health_cdcplaces.csv')
    return df['TractFIPS'][df.PlaceName == 'Atlanta']


def download_TIGERS(year, layer):
    """
    :param year: year of interest, 2019 as default
    :param layer: 'place' or 'tract'
    :return: none
    """

    url = "https://www2.census.gov/geo/tiger/TIGER{0}/{1}/tl_{2}_13_{3}.zip".format(year,layer.upper(), year, layer)
    path = '../data/raw/tl_{0}_13_{1}'.format(year, layer)
    urllib.request.urlretrieve(url, path + '.zip')
    with zipfile.ZipFile(path+ '.zip', 'r') as zip_ref:
        zip_ref.extractall(path)


def clean_TIGERS(year, place_name = 'Atlanta'):
    """

    :param year:
    :param layer:
    :return:
    """

    path_tract = '../data/raw/tl_{0}_13_tract/tl_{1}_13_tract.shp'.format(year, year)


    # find census tracts within the place boundary
    shp_tract = gpd.read_file(path_tract)
    shp_tract = shp_tract.loc[shp_tract.GEOID.isin(load_AtlantaCT_FIPS().astype('str')),['COUNTYFP','GEOID','geometry']]

    os.mkdir('../data/cleaned/tract_{0}_{1}'.format(place_name, year))
    shp_tract.to_file('../data/cleaned/tract_{0}_{1}/tract_{0}_{1}.shp'.format(place_name, year))




def clean_CDCPlaces(tractFIPS):
    """

    :param tractFIPS:
    :return:
    """

    file_name = [file for file in os.listdir('../data/raw') if 'health_cdcplaces' in file]
    acs = pd.read_csv('../data/raw/2019_SES_acs.csv').loc[:,['TractFIPS', 'pop_total']]
    acs['TractFIPS'] = acs['TractFIPS'].astype('str')

    # find common column names
    column_list = []
    df_list = []
    for file in file_name:
        df = pd.read_csv('../data/raw/{0}'.format(file))
        column_list.append(df.columns.values)
        df_list.append(df)

    new_column_list = reduce(np.intersect1d, (column_list))
    f = np.frompyfunc(lambda x: '_CrudePrev' in x, 1, 1)
    new_column_list = new_column_list[np.where(f(new_column_list))]

    melt_df_list = []
    for i,df in enumerate(df_list):
        df = df.loc[df.TractFIPS.isin(tractFIPS),
                    np.append(['TractFIPS'],new_column_list)].reset_index(drop = True)
        df['TractFIPS'] = df['TractFIPS'].astype('str')
        #df = df.merge(acs, on = 'TractFIPS', how = 'left')
        #for c in df.columns.values[1:len(df.columns.values)-1]:
            #wm = np.average(a = df[c], weights = df['pop_total'])

        df.to_csv('../data/cleaned/{0}'.format(file_name[i]), index = False)

        # melting df
        df_melt = df.melt(id_vars=['TractFIPS'], value_vars=df.columns.values[1:])
        df_melt['year'] = file_name[i].split('_')[0]
        melt_df_list.append(df_melt)


    pd.concat(melt_df_list).reset_index(drop = True).to_csv('../data/cleaned/health_cdcplaces_melt.csv', index = False)

