#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for scrapername.

'''
from os.path import join

import pytest
from hdx.hdx_configuration import Configuration
from idmc import generate_dataset_and_showcase, get_countriesdata


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
                elif url == 'http://lala/aggregated/disaster_data?iso3=AFG':
                    def fn():
                        return {'results': [{'iso3': 'AFG', 'new_displacements': 3430, 'year': 2008},
                                            {'iso3': 'AFG', 'new_displacements': 28435, 'year': 2009},
                                            {'iso3': 'AFG', 'new_displacements': 71000, 'year': 2010},
                                            {'iso3': 'AFG', 'new_displacements': 3000, 'year': 2011},
                                            {'iso3': 'AFG', 'new_displacements': 29519, 'year': 2012},
                                            {'iso3': 'AFG', 'new_displacements': 15170, 'year': 2013},
                                            {'iso3': 'AFG', 'new_displacements': 13125, 'year': 2014},
                                            {'iso3': 'AFG', 'new_displacements': 70948, 'year': 2015},
                                            {'iso3': 'AFG', 'new_displacements': 7394, 'year': 2016}]}
                    response.json = fn
                    response.url = '%s&ci=123' % url
                return response
        return Download()

    def test_get_countriesdata(self, downloader):
        countriesdata = get_countriesdata('http://haha/', downloader)
        assert countriesdata == [TestIDMC.countrydata]

    def test_generate_dataset(self, configuration, downloader):
        base_url = Configuration.read()['base_url']
        dataset, showcase = generate_dataset_and_showcase(base_url, downloader, TestIDMC.countrydata, Configuration.read()['endpoints'])
        assert dataset == {'name': 'idmc-data-for-afghanistan', 'title': 'IDMC data for Afghanistan', 'groups': [{'name': 'afg'}],
                           'tags': [{'name': 'population'}, {'name': 'displacement'}, {'name': 'idmc'}],
                           'data_update_frequency': '1', 'dataset_date': '01/01/2008-12/31/2016'}
        resources = dataset.get_resources()
        assert resources == [{'format': 'json', 'name': 'aggregated_disaster_data',
                              'url': 'http://lala/aggregated/disaster_data?iso3=AFG&ci=123', 'description': 'Aggregated disaster data'}]
        assert showcase == {'name': 'idmc-data-for-afghanistan-showcase', 'url': 'http://www.internal-displacement.org/countries/Afghanistan/',
                            'tags': [{'name': 'population'}, {'name': 'displacement'}, {'name': 'idmc'}],
                            'image_url': 'http://www.internal-displacement.org/themes/idmc-flat/img/logo.png',
                            'notes': 'Click the image on the right to go to the IDMC summary page for the Afghanistan dataset',
                            'title': 'IDMC Afghanistan Summary Page'}

