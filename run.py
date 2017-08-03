#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

import sys
from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download

from idmc import generate_dataset, get_countriesdata

from hdx.facades.hdx_scraperwiki import facade

logger = logging.getLogger(__name__)


def main():
    """Generate dataset and create it in HDX"""

    base_url = Configuration.read()['base_url']
    downloader = Download(extra_params_yaml=join(expanduser("~"), 'idmc.yml'))
    countriesdata = get_countriesdata(base_url, downloader)
    logger.info('Number of datasets to upload: %d' % len(countriesdata))
    for countrydata in countriesdata:
        dataset = generate_dataset(base_url, downloader, countrydata, Configuration.read()['endpoints'])
        if dataset:
            dataset.update_from_yaml()
            dataset.create_in_hdx()
            sys.exit(0)

if __name__ == '__main__':
    facade(main, hdx_site='test', project_config_yaml=join('config', 'project_configuration.yml'))

