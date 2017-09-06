#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for scrapername.

'''
from os.path import join

import pytest
from hdx.hdx_configuration import Configuration
from idmc import generate_country_dataset_and_showcase, get_countriesdata, generate_indicator_datasets_and_showcase


class TestIDMC:
    countrydata = {'iso3': 'AFG', 'geo_name': 'Afghanistan'}

    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True,
                             project_config_yaml=join('tests', 'config', 'project_configuration.yml'))

    @pytest.fixture(scope='function')
    def downloader(self):
        class Response:
            def json(self):
                pass

        class Download:
            @staticmethod
            def download(url):
                response = Response()
                if url == 'http://haha/countries':
                    def fn():
                        return {'results': [TestIDMC.countrydata]}
                    response.json = fn
                elif url == 'http://lala/conflict_data':
                    def fn():
                        return {'results': [{'iso3': 'AFG', 'geo_name': 'Afghanistan', 'iso': 'AF',
                                             'stock_displacement': 297000, 'stock_displacement_source': 'IDMC',
                                             'new_displacements': 0, 'year': 2009, 'new_displacements_source': 'IDMC'},
                                            {'iso3': 'CIV', 'geo_name': "Cote d'Ivoire", 'iso': 'CI',
                                             'stock_displacement': 303150, 'stock_displacement_source': 'IDMC',
                                             'new_displacements': 3150, 'year': 2015,
                                             'new_displacements_source': 'IDMC'},
                                            {'iso3': 'COG', 'geo_name': 'Congo, Rep', 'iso': 'CG',
                                             'stock_displacement': 7800, 'stock_displacement_source': 'IDMC',
                                             'new_displacements': 0, 'year': 2015, 'new_displacements_source': 'IDMC'}]}

                    response.json = fn
                    response.url = '%s&ci=123' % url
                    return response
                elif url == 'http://lala/conflict_data?iso3=AFG':
                    def fn():
                        return {'results': [{'iso3': 'AFG', 'geo_name': 'Afghanistan', 'iso': 'AF',
                                             'stock_displacement': 297000, 'stock_displacement_source': 'IDMC',
                                             'new_displacements': 0, 'year': 2009, 'new_displacements_source': 'IDMC'},
                                            {'iso3': 'AFG', 'geo_name': 'Afghanistan', 'iso': 'AF',
                                             'stock_displacement': 492000, 'stock_displacement_source': 'IDMC',
                                             'new_displacements': 100400, 'year': 2012,
                                             'new_displacements_source': 'IDMC'},
                                            {'iso3': 'AFG', 'geo_name': 'Afghanistan', 'iso': 'AF',
                                             'stock_displacement': 1174306, 'stock_displacement_source': 'IDMC',
                                             'new_displacements': 335409, 'year': 2015,
                                             'new_displacements_source': 'IDMC'}]}
                    response.json = fn
                    response.url = '%s&ci=123' % url
                return response

            @staticmethod
            def download_csv_key_value(url):
                if url == 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRubZgyjd7Az7Vgaxb5lWFpjojmjYZRlcVaVqYBEuEmpIojnuVn0nJG6DAJUaIzn0NdVhAkQuBw5t8q/pub?gid=2096291259&single=true&output=csv':
                    return {
                        'Indicator Name': 'Internally displaced persons - IDPs (people displaced by conflict and violence)',
                        'Long definition': 'Description',
                        'Statistical concept and methodology': 'Methodology',
                        'Limitations and exceptions': 'Caveats'
                    }

        return Download()

    def test_get_countriesdata(self, downloader):
        countriesdata = get_countriesdata('http://haha/', downloader)
        assert countriesdata == [TestIDMC.countrydata]

    def test_generate_datasets_and_showcase(self, configuration, downloader):
        base_url = Configuration.read()['base_url']
        datasets, showcase = generate_indicator_datasets_and_showcase(base_url, downloader, Configuration.read()['endpoints'], Configuration.read()['tags'])
        assert datasets == {'conflict_data': {'title': 'Internally displaced persons - IDPs (people displaced by conflict and violence)',
                            'groups': [{'name': 'world'}],
                            'dataset_date': '01/01/2009-12/31/2015',
                            'maintainer': '196196be-6037-4488-8b71-d786adf4c081', 'data_update_frequency': '1',
                         'tags': [{'name': 'population'}, {'name': 'displacement'}, {'name': 'idmc'}],
                             'name': 'internally-displaced-persons-idps-people-displaced-by-conflict-and-violence',
                             'notes': "Description\n\nContains data from IDMC's [data portal](https://github.com/idmc-labs/IDMC-Platform-API/wiki).",
                             'methodology_other': 'Methodology',
                             'caveats': 'Caveats',
                             'owner_org': '647d9d8c-4cac-4c33-b639-649aad1c2893'}}
        resources = datasets['conflict_data'].get_resources()
        assert resources == [{'description': 'Internally displaced persons - IDPs (people displaced by conflict and violence)',
                              'format': 'json', 'name': 'conflict_data', 'url': 'http://lala/conflict_data&ci=123'}]
        assert showcase == {'tags': [{'name': 'population'}, {'name': 'displacement'}, {'name': 'idmc'}],
                            'url': 'http://www.internal-displacement.org/global-report/grid2017/',
                            'name': 'idmc-global-report-on-internal-displacement',
                            'title': 'IDMC Global Report on Internal Displacement',
                            'notes': 'Click the image on the right to go to the IDMC Global Report on Internal Displacement',
                            'image_url': 'http://www.internal-displacement.org/themes/idmc-flat/img/logo.png'}
        dataset, showcase = generate_country_dataset_and_showcase(base_url, downloader, datasets, TestIDMC.countrydata, Configuration.read()['endpoints'], Configuration.read()['tags'])
        assert dataset == {'name': 'idmc-idp-data-for-afghanistan', 'title': 'Afghanistan - Internally displaced persons - IDPs (people displaced by conflict and violence)',
                           'groups': [{'name': 'afg'}], 'maintainer': '196196be-6037-4488-8b71-d786adf4c081',
                           'dataset_date': '01/01/2009-12/31/2015',
                           'tags': [{'name': 'population'}, {'name': 'displacement'}, {'name': 'idmc'}],
                           'data_update_frequency': '1',
                           'notes': "Description\n\nContains data from IDMC's [data portal](https://github.com/idmc-labs/IDMC-Platform-API/wiki).",
                           'methodology_other': 'Methodology',
                           'caveats': 'Caveats',
                           'owner_org': '647d9d8c-4cac-4c33-b639-649aad1c2893'}

        resources = dataset.get_resources()
        assert resources == [{'format': 'json', 'url': 'http://lala/conflict_data?iso3=AFG&ci=123',
                              'name': 'conflict_data',
                              'description': 'Internally displaced persons - IDPs (people displaced by conflict and violence)'}]
        assert showcase == {'tags': [{'name': 'population'}, {'name': 'displacement'}, {'name': 'idmc'}],
                            'notes': 'Click the image on the right to go to the IDMC summary page for the Afghanistan dataset',
                            'image_url': 'http://www.internal-displacement.org/themes/idmc-flat/img/logo.png',
                            'url': 'http://www.internal-displacement.org/countries/Afghanistan/',
                            'title': 'IDMC Afghanistan Summary Page', 'name': 'idmc-idp-data-for-afghanistan-showcase'}

