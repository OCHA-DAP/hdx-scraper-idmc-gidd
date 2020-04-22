#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir, multiple_progress_storing_tempdir, get_temp_dir

from idmc import generate_indicator_datasets_and_showcase, generate_country_dataset_and_showcase

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-idmc'


def main():
    """Generate dataset and create it in HDX"""

    with Download() as downloader:
        indicators = Configuration.read()['indicators']
        tags = Configuration.read()['tags']
        folder = get_temp_dir('IDMC')
        datasets, showcase, headersdata, countriesdata = generate_indicator_datasets_and_showcase(downloader, folder, indicators, tags)
        showcase_not_added = True
        countries = [{'iso3': x} for x in sorted(countriesdata)]

        logger.info('Number of indicator datasets to upload: %d' % len(indicators))
        logger.info('Number of country datasets to upload: %d' % len(countries))
        for i, info, nextdict in multiple_progress_storing_tempdir('IDMC', [indicators, countries],
                                                                   ['name', 'iso3']):
            folder = info['folder']
            batch = info['batch']
            if i == 0:
                if showcase_not_added:
                    showcase.create_in_hdx()
                    showcase_not_added = False
                dataset = datasets[nextdict['name']]
                dataset.update_from_yaml()
                dataset.generate_resource_view(join('config', nextdict['resourceview']))
                dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False, updated_by_script='HDX Scraper: IDMC', batch=batch)
                showcase.add_dataset(dataset)
            else:
                countryiso = nextdict['iso3']
                countrydata = countriesdata[countryiso]
                dataset, showcase, bites_disabled = \
                    generate_country_dataset_and_showcase(downloader, folder, headersdata, countryiso, countrydata, datasets, tags)
                if dataset:
                    dataset.update_from_yaml()
                    dataset.generate_resource_view(bites_disabled=bites_disabled)
                    dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False, updated_by_script='HDX Scraper: IDMC', batch=batch)
                    resources = dataset.get_resources()
                    resource_ids = [x['id'] for x in sorted(resources, key=lambda x: len(x['name']), reverse=True)]
                    dataset.reorder_resources(resource_ids, hxl_update=False)


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))

