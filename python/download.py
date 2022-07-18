from data_util import *

download_CDCPlaces([2018,2019,2020,2021])

download_ACS(year = 2019, api_key = 'd54b04fce5ead0b754d8951da1ced097f3d050e1',
                 table = 'detailed',
                 state_id = '13', county_id = ['135'],
                 acs5_variable_list = ("B01003_001E", "B19013_001E"), acs_name_list = ('pop_total', 'medhhinc_all')
                 )

download_TIGERS(year = '2019', layer = 'tract')
download_TIGERS(year = '2020', layer = 'tract')
download_TIGERS(year = '2021', layer = 'tract')

download_TIGERS(year = '2019', layer = 'place')
download_TIGERS(year = '2020', layer = 'place')
download_TIGERS(year = '2021', layer = 'place')

clean_TIGERS(year = '2019', place_name = 'Atlanta')
clean_TIGERS(year = '2020', place_name = 'Atlanta')
clean_TIGERS(year = '2021', place_name = 'Atlanta')

clean_CDCPlaces(tractFIPS=load_AtlantaCT_FIPS())
