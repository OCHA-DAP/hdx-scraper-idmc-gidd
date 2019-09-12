#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir

from idmc import generate_indicator_datasets_and_showcase, generate_country_dataset_and_showcase, generate_resource_view

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-idmc'


def main():
    """Generate dataset and create it in HDX"""

    with temp_dir('idmc') as folder:
        with Download() as downloader:
            displacement_url = Configuration.read()['displacement_url']
            disaster_url = Configuration.read()['disaster_url']
            endpoints = Configuration.read()['endpoints']
            tags = Configuration.read()['tags']
            datasets, showcase, headersdata, countriesdata = generate_indicator_datasets_and_showcase(displacement_url, disaster_url, downloader, folder, endpoints, tags)
            showcase.create_in_hdx()
            for endpoint in datasets:
                dataset = datasets[endpoint]
                dataset.update_from_yaml()
                dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False)
                if endpoint == 'disaster_data':
                    path = join('config', 'hdx_resource_view_static_disaster.yml')
                else:
                    path = None
                resource_view = generate_resource_view(dataset, path=path)
                resource_view.create_in_hdx()
                showcase.add_dataset(dataset)

            for countryiso in countriesdata:
                dataset, showcase, empty_col = generate_country_dataset_and_showcase(downloader, folder, headersdata, countryiso, countriesdata[countryiso], datasets, tags)
                if dataset:
                    dataset.update_from_yaml()
                    dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False)
                    resources = dataset.get_resources()
                    resource_ids = [x['id'] for x in sorted(resources, key=lambda x: len(x['name']), reverse=True)]
                    dataset.reorder_resources(resource_ids, hxl_update=False)
                    resource_view = generate_resource_view(dataset, empty_col=empty_col)
                    resource_view.create_in_hdx()
                    if showcase:
                        showcase.create_in_hdx()
                        showcase.add_dataset(dataset)


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))

