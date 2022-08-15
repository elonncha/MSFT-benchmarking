from data_util import *

place_name = 'Atlanta'
neighborhood_name = ['Center Hill', 'Grove Park', 'Knight Park/Howell Station', 'Historic Westin Heights/Bankhead']
years = ['2019','2020','2021']

''' health_cdcplaces '''
download_CDCPlaces(years)
clean_CDCPlaces(tractFIPS=load_Tract_FIPS(place_name = place_name))
melt_df('health_cdcplaces')



''' TIGERS SHP & NEIGHBORHOOD SHP '''
for y in years:
    download_TIGERS(year=y, layer='tract')
    clean_TIGERS(year=y, place_name = place_name)

clean_neighborhood_shp(shp_address = '../data/raw/Atlanta_Neighborhoods/Atlanta_Neighborhoods.shp',
                       neighborhood_name = neighborhood_name)


''' ACS '''
acs5st_variable_list = ['S0601_C01_014E', 'S0601_C01_015E', 'S0601_C01_017E', 'S0601_C01_021E','S1701_C02_001E', "S2201_C01_001E"]
acs5st_name_list = ['race_pc_white', 'race_pc_black', 'race_pc_asian', 'race_pc_hispanic','pov_total','snaphh_total']

acs5_variable_list = ["B01003_001E", "B19013_001E"]
acs5_name_list = ['pop_total', 'medhhinc_all']

acs5dp_variable_list = ['DP02_0016E', 'DP02_0072PE', 'DP02_0063PE', 'DP02_0067PE', 'DP02_0068PE', 'DP02_0152PE', 'DP02_0153PE',
                        'DP03_0009PE', 'DP03_0019PE', 'DP03_0020PE', 'DP03_0021PE', 'DP03_0025E', 'DP03_0128PE', 'DP03_0099PE',
                        'DP04_0001E','DP04_0004E','DP04_0005E',
                        'DP04_0046PE', 'DP04_0047PE',
                        'DP04_0089E', 'DP04_0134E', 'DP04_0142PE',
                        'DP05_0018E', 'DP05_0019PE']
acs5dp_name_list = ['housing_avghhsize', 'housing_pc_disability', 'housing_pc_somecollege', 'housing_pc_highschool', 'housing_pc_bachelor','housing_pc_computer', 'housing_pc_internet',
                    'housing_pc_unemployment', 'housing_pc_drovealone', 'housing_pc_carpool', 'housing_pc_publictransit', 'housing_mtimetowork', 'housing_pc_poverty', 'housing_pc_uninsured',
                    'housing_total','housing_pc_ownervacant','housing_pc_rentervacant',
                    'housing_pc_owneroccupied', 'housing_pc_renteroccupied',
                    'housing_medval','housing_medgrossrent', 'housing_pc_GRAPI35',
                    'housing_medianage', 'housing_pc_under18']

download_ACS(year = 2019, api_key = 'd54b04fce5ead0b754d8951da1ced097f3d050e1',
                 table = 'subject',
                 state_id = '13', county_id = ['121', '089'],
                 acs5_variable_list = acs5st_variable_list, acs_name_list = acs5st_name_list
                 )
download_ACS(year = 2019, api_key = 'd54b04fce5ead0b754d8951da1ced097f3d050e1',
                 table = 'detailed',
                 state_id = '13', county_id = ['121', '089'],
                 acs5_variable_list = acs5_variable_list, acs_name_list = acs5_name_list
                 )
download_ACS(year = 2019, api_key = 'd54b04fce5ead0b754d8951da1ced097f3d050e1',
                 table = 'profile',
                 state_id = '13', county_id = ['121', '089'],
                 acs5_variable_list = acs5dp_variable_list, acs_name_list = acs5dp_name_list
                 )

clean_ACS(tractFIPS=load_Tract_FIPS(place_name = place_name), year = '2019')


''' Eviction '''
clean_Eviction(tractFIPS=load_Tract_FIPS(place_name = place_name), year = '2020')



### output
x = combine_output(df_file = '../data/cleaned/2021_health_cdcplaces.csv',
               crosswalk_file = '../data/cleaned/crosswalk.csv',
               pop_file = '../data/cleaned/2019_SES_acs.csv')
x.set_index('TractFIPS').transpose().to_csv('../data/toMSFT/health.csv')


x = combine_output(df_file = '../data/cleaned/2019_SES_acs.csv',
               crosswalk_file = '../data/cleaned/crosswalk.csv',
               pop_file = '../data/cleaned/2019_SES_acs.csv')
x.set_index('TractFIPS').transpose().to_csv('../data/toMSFT/acs.csv')


x = combine_output(df_file = '../data/cleaned/2020_Trans_multi.csv',
               crosswalk_file = '../data/cleaned/crosswalk.csv',
               pop_file = '../data/cleaned/2019_SES_acs.csv')
x.set_index('TractFIPS').transpose().to_csv('../data/toMSFT/transportation.csv')



x = combine_output(df_file = '../data/cleaned/2020_housing_eviction.csv',
               crosswalk_file = '../data/cleaned/crosswalk.csv',
               pop_file = '../data/cleaned/2019_SES_acs.csv')
x.set_index('TractFIPS').transpose().to_csv('../data/toMSFT/eviction.csv')