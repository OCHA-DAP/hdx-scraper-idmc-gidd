#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download

from idmc import generate_indicator_datasets_and_showcase, generate_country_dataset_and_showcase, get_countriesdata

from hdx.facades import logging_kwargs
logging_kwargs['smtp_config_yaml'] = join('config', 'smtp_configuration.yml')

from hdx.facades.hdx_scraperwiki import facade

logger = logging.getLogger(__name__)


def main():
    """Generate dataset and create it in HDX"""

    base_url = Configuration.read()['base_url']
    with Download(extra_params_yaml=join(expanduser("~"), 'idmc.yml')) as downloader:
        endpoints = Configuration.read()['endpoints']
        tags = Configuration.read()['tags']
        datasets, showcase = generate_indicator_datasets_and_showcase(base_url, downloader, endpoints, tags)
        showcase.create_in_hdx()
        for dataset in datasets.values():
            dataset.update_from_yaml()
            dataset.create_in_hdx()
            showcase.add_dataset(dataset)

        countriesdata = get_countriesdata(base_url, downloader)
        logger.info('Number of datasets to upload: %d' % len(countriesdata))
        for countrydata in countriesdata:
            dataset, showcase = generate_country_dataset_and_showcase(base_url, downloader, datasets, countrydata, endpoints, tags)
            if dataset:
                dataset.update_from_yaml()
                dataset.create_in_hdx()
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)


if __name__ == '__main__':
    facade(main, hdx_site='feature', user_agent_config_yaml=join(expanduser('~'), '.idmcuseragent.yml'), project_config_yaml=join('config', 'project_configuration.yml'))

