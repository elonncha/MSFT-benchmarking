import os
import urllib.request
from census import Census
import pandas as pd
import urllib.request
import zipfile
import geopandas as gpd
import numpy as np
from functools import reduce


### housekeeping
def load_Tract_FIPS(place_name):
    '''
    return a list of tractFIPS within the place of interest (e.g. 'Atlanta')

    :param place_name: name of place
    :return: a panda series of tractFIPS
    '''
    df = pd.read_csv('../data/raw/2019_health_cdcplaces.csv')
    return df['TractFIPS'][df.PlaceName == place_name]


def melt_df(dataframe):
    '''
    melt columns of cleaned dataframes into rows

    :param dataframe: e.g. 'health_cdcplaces'
    :return: NONE
    '''

    file_name = [file for file in os.listdir('../data/cleaned') if dataframe in file]
    df_list = [pd.read_csv('../data/cleaned/{0}'.format(f)) for f in file_name]
    melt_df_list = []
    for i, df in enumerate(df_list):
        df_melt = df.melt(id_vars=['TractFIPS'], value_vars=df.columns.values[1:])
        df_melt['year'] = file_name[i].split('_')[0]
        melt_df_list.append(df_melt)

    pd.concat(melt_df_list).reset_index(drop = True).to_csv('../data/cleaned/{0}_melt.csv'.format(dataframe), index = False)


def ct_neighborhood_crosswalk(ct_address, neighborhood_address):
    '''

    :param ct_address: file address of census tract shp
    :param neighborhood_address: file address of neighborhood shp
    :return:
    '''

    # neighborhood_address = '../data/cleaned/neighborhood/neighborhood.shp'
    # ct_address = '../data/cleaned/tract_Atlanta_2019/tract_Atlanta_2019.shp'

    ct, nbh = gpd.read_file(ct_address), gpd.read_file(neighborhood_address)
    nbh = nbh.to_crs(crs = ct.crs)
    crswk = gpd.overlay(ct, nbh, how = 'intersection')
    crswk['area'] = crswk['geometry'].to_crs(epsg = 3857).area

    crswk = crswk.loc[:,['GEOID','NAME','area']].\
                rename(columns = {'GEOID':'TractFIPS'}).\
                join(crswk.groupby('NAME').agg({'area':'sum'}).
                     rename(columns = {'area':'a'}), on = 'NAME', how = 'left')

    crswk['percent'] = crswk['area'] / crswk['a']

    crswk.loc[:,['TractFIPS','NAME','percent']].to_csv('../data/cleaned/crosswalk.csv',index = False)







### dataset
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
        drop(columns=['tract','state','county']). \
        reset_index(drop=True)

    # rename columns and write as csv
    for i, code in enumerate(acs5_variable_list):
        output = output.rename(columns={code: acs_name_list[i]})

    output.to_csv('../data/raw/{0}_SES_acs_{1}.csv'.format(year, table), index = False)


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


def clean_neighborhood_shp(shp_address, neighborhood_name):
    '''

    :param shp_address: shp file
    :param neighborhood_name: neighborhoods of interest
    :return:
    '''
    #shp_address = '../data/raw/Atlanta_Neighborhoods/Atlanta_Neighborhoods.shp'
    # neighborhood_name = ['Center Hill', 'Grove Park', 'Knight Park/Howell Station', 'Historic Westin Heights/Bankhead']

    shp = gpd.read_file(shp_address)
    shp = shp.loc[shp.NAME.isin(neighborhood_name),['OBJECTID','NAME','geometry']]

    shp.to_file('../data/cleaned/neighborhood/neighborhood.shp')


def clean_TIGERS(year, place_name):
    """

    :param year:
    :param place_name:
    :return:
    """

    path_tract = '../data/raw/tl_{0}_13_tract/tl_{1}_13_tract.shp'.format(year, year)


    # find census tracts within the place boundary
    shp_tract = gpd.read_file(path_tract)
    shp_tract = shp_tract.loc[shp_tract.GEOID.isin(load_Tract_FIPS(place_name).astype('str')),['COUNTYFP','GEOID','geometry']]

    os.mkdir('../data/cleaned/tract_{0}_{1}'.format(place_name, year))
    shp_tract.to_file('../data/cleaned/tract_{0}_{1}/tract_{0}_{1}.shp'.format(place_name, year))


def clean_CDCPlaces(tractFIPS):
    """

    :param tractFIPS: tract fips of interest
    :return:
    """

    file_name = [file for file in os.listdir('../data/raw') if 'health_cdcplaces' in file]

    # find common column names
    column_list, df_list = [], []
    for file in file_name:
        df = pd.read_csv('../data/raw/{0}'.format(file))
        column_list.append(df.columns.values)
        df_list.append(df)

    new_column_list = reduce(np.intersect1d, (column_list))
    f = np.frompyfunc(lambda x: '_CrudePrev' in x, 1, 1)
    new_column_list = new_column_list[np.where(f(new_column_list))]

    for i,df in enumerate(df_list):
        df = df.loc[df.TractFIPS.isin(tractFIPS),
                    np.append(['TractFIPS'],new_column_list)].reset_index(drop = True)
        df['TractFIPS'] = df['TractFIPS'].astype('str')
        df.to_csv('../data/cleaned/{0}'.format(file_name[i]), index = False)


def clean_ACS(tractFIPS, year):
    file_name = [file for file in os.listdir('../data/raw') if '{0}_SES_acs'.format(year) in file]
    df_list = [pd.read_csv('../data/raw/'+ f) for f in file_name]

    df = reduce(lambda x, y: pd.merge(x, y, on='TractFIPS'), df_list)
    df = df.loc[df.TractFIPS.isin(tractFIPS),:]
    tractFIPSCol = df['TractFIPS']
    df = df.drop(columns = ['TractFIPS'])
    df.insert(0,'TractFIPS', tractFIPSCol.values)

    df.to_csv('../data/cleaned/{0}_SES_acs.csv'.format(year), index = False)


def clean_Eviction(tractFIPS, year):
    df = pd.read_csv('../data/raw/{0}_housing_eviction.csv'.format(year))
    df = df.loc[df.TractFIPS.isin(tractFIPS), :]

    pop = pd.read_csv('../data/cleaned/2019_SES_acs.csv').loc[:,['TractFIPS','housing_total']]

    # compute eviction cases per household
    df = df.merge(pop, on = 'TractFIPS')
    df['eviction_percap'] = df['eviction'] / df['housing_total']
    df = df.drop(columns = ['housing_total'])

    df.to_csv('../data/cleaned/{0}_housing_eviction.csv'.format(year), index=False)


def clean_tax_parcel():
    fulton_parcel = gpd.read_file('../data/raw/Fulton_Tax_Parcels/Tax_Parcels.shp').loc[:,['ParcelID','Owner','TotAssess','TotAppr','LUCode','ClassCode','LandAcres','geometry']]
    nbh = gpd.read_file('../data/cleaned/neighborhood/neighborhood.shp').to_crs(crs = fulton_parcel.crs)


    nbh_parcel = fulton_parcel.loc[nbh.contains(fulton_parcel),:]
    membership = gpd.overlay(fulton_parcel, nbh, how = 'intersection')

### statistics
def compute_weighted_avg(df_file, pop_file):
    '''

    :param df_file:
    :param pop_file:
    :return:
    '''

    # df_file = '../data/cleaned/2019_SES_acs.csv'
    # pop_file = '../data/cleaned/2019_SES_acs.csv'

    # load and merge two datasets
    df = pd.read_csv(df_file)
    if 'pop_total' in df.columns.values:
        df = df.drop(columns = ['pop_total'])
    pop = pd.read_csv(pop_file).loc[:,['TractFIPS','pop_total']]

    # check if values are non-negative (acs codes NA as -666666)
    df = df[(df >= 0).all(1)]

    df['TractFIPS'] = df['TractFIPS'].astype('str')
    pop['TractFIPS'] = pop['TractFIPS'].astype('str')

    df = df.merge(pop, on='TractFIPS', how = 'left')

    # extract weight
    w = df['pop_total']
    df = df.drop(columns = ['pop_total'])


    avg = df.dropna().groupby([True]*len(df)).agg(lambda x: np.average(x, weights = w)).reset_index()
    avg = avg.rename(columns = {'index':'TractFIPS'})
    avg['TractFIPS'] = 'cityAVG'

    return(avg)


def crosswalk_multiplier(df_file, crosswalk_file):
    # df_file = '../data/cleaned/2020_health_cdcplaces.csv'
    # crosswalk_file = '../data/cleaned/crosswalk.csv'

    df = pd.read_csv(df_file)
    crwk = pd.read_csv(crosswalk_file)

    # check if values are non-negative
    df = df[(df >= 0).all(1)]

    df['TractFIPS'] = df['TractFIPS'].astype('str')
    crwk['TractFIPS'] = crwk['TractFIPS'].astype('str')


    df = df.merge(crwk, on = 'TractFIPS', how = 'right')
    w, name = df['percent'], df['NAME']
    df = df.drop(columns = ['TractFIPS', 'percent', 'NAME'])
    df.insert(0, 'TractFIPS', name)

    for c_index in range(1,df.shape[1]):
        df.iloc[:,c_index] = df.iloc[:,c_index] * w

    df = df.dropna().groupby('TractFIPS').agg(lambda x: sum(x)).reset_index()

    return(df)



def combine_output(df_file, crosswalk_file, pop_file):
    neighrborhood_output = crosswalk_multiplier(df_file, crosswalk_file)
    city_output = compute_weighted_avg(df_file, pop_file)

    df = pd.read_csv(df_file)
    CT85_output = df.loc[df.TractFIPS == 13121008500,:]


    return(pd.concat([CT85_output, neighrborhood_output, city_output]).reset_index(drop = True))