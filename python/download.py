from data_util import *


''' health_cdcplaces '''
download_CDCPlaces([2018,2019,2020,2021])
clean_CDCPlaces(tractFIPS=load_AtlantaCT_FIPS())
melt_df('health_cdcplaces')

''' TIGERS SHP & NEIGHBORHOOD SHP '''
download_TIGERS(year = '2019', layer = 'tract')
download_TIGERS(year = '2020', layer = 'tract')
download_TIGERS(year = '2021', layer = 'tract')

clean_TIGERS(year = '2019', place_name = 'Atlanta')
clean_TIGERS(year = '2020', place_name = 'Atlanta')
clean_TIGERS(year = '2021', place_name = 'Atlanta')


''' ACS '''
acs5st_variable_list = ['S0601_C01_014E', 'S0601_C01_015E', 'S0601_C01_017E', 'S0601_C01_021E',  # race/ethnicity precentage
                        'S1701_C02_001E', "S2201_C01_001E", "S2704_C03_006E"
                        ]
acs5st_name_list = ['race_pc_white', 'race_pc_black', 'race_pc_asian', 'race_pc_hispanic',
                    'pov_total','snaphh_total','medicaid_pc_covered'
                    ]

acs5_variable_list = ["B01003_001E", "B19013_001E"]
acs5_name_list = ['pop_total', 'medhhinc_all']

acs5dp_variable_list = ['DP04_0001E','DP04_0003PE',
                        'DP04_0046PE', 'DP04_0047PE',
                        'DP04_0089E', 'DP04_0134E', 'DP04_0141PE', 'DP04_0142PE']
acs5dp_name_list = ['housing_total','housing_pc_vacant',
                    'housing_pc_owneroccupied', 'housing_pc_renteroccupied',
                    'housing_medval','housing_medgrossrent', 'housing_pc_renttoinc_3034', 'housing_pc_renttoinc_over35']

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

clean_ACS(tractFIPS=load_AtlantaCT_FIPS(), year = '2019')








x = combine_output(df_file = '../data/cleaned/2020_health_cdcplaces.csv',
               crosswalk_file = '../data/cleaned/crosswalk.csv',
               pop_file = '../data/cleaned/2019_SES_acs.csv')
x.set_index('TractFIPS').transpose().to_csv('../data/toMSFT/health.csv')


x = combine_output(df_file = '../data/cleaned/2019_SES_acs.csv',
               crosswalk_file = '../data/cleaned/crosswalk.csv',
               pop_file = '../data/cleaned/2019_SES_acs.csv')
x.set_index('TractFIPS').transpose().to_csv('../data/toMSFT/acs.csv')