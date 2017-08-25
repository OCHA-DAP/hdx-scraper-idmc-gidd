#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
IDMC:
------------

Reads IDMC JSON and creates datasets.

"""

import logging

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.utilities.location import Location
from slugify import slugify

logger = logging.getLogger(__name__)


def get_countriesdata(base_url, downloader):
    response = downloader.download('%scountries' % base_url)
    jsonresponse = response.json()
    return jsonresponse['results']


def get_title(endpoint):
    description = endpoint.split('_')
    description[0] = '%s%s' % (description[0][0].upper(), description[0][1:])
    return ' '.join(description)


def get_resource(endpoint, url):
    resource = {
        'name': endpoint,
        'format': 'json',
        'url': url,
        'description': get_title(endpoint)
    }
    return resource


def get_dataset(title, tags):
    logger.info('Creating dataset: %s' % title)
    slugified_name = slugify(title).lower()
    dataset = Dataset({
        'name': slugified_name,
        'title': title
    })
    dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    dataset.set_organization('hdx')
    dataset.set_expected_update_frequency('Every day')
    tags = ['population', 'displacement', 'idmc']
    dataset.add_tags(tags)
    return dataset


def generate_indicator_datasets_and_showcase(base_url, downloader, endpoints, tags):
    datasets = list()
    for endpoint in endpoints:
        title = 'IDMC %s' % get_title(endpoint)
        dataset = get_dataset(title, tags)
        dataset.add_other_location('world')
        url = '%s%s' % (base_url, endpoint)
        response = downloader.download(url)
        json = response.json()
        earliest_year = 10000
        latest_year = 0
        for result in json['results']:
            year = result.get('year')
            if year is None:
                continue
            if year > latest_year:
                latest_year = year
            if year < earliest_year:
                earliest_year = year

        dataset.add_update_resource(get_resource(endpoint, response.url))
        dataset.set_dataset_year_range(earliest_year, latest_year)
        datasets.append(dataset)

    title = 'IDMC Global Report on Internal Displacement'
    slugified_name = slugify(title).lower()
    showcase = Showcase({
        'name': slugified_name,
        'title': title,
        'notes': 'Click the image on the right to go to the %s' % title,
        'url': 'http://www.internal-displacement.org/global-report/grid2017/',
        'image_url': 'http://www.internal-displacement.org/themes/idmc-flat/img/logo.png'
    })
    showcase.add_tags(tags)
    return datasets, showcase


def generate_country_dataset_and_showcase(base_url, downloader, countrydata, endpoints, tags):
    countryname = countrydata['geo_name']
    title = 'IDMC data for %s' % countryname
    dataset = get_dataset(title, tags)
    countryiso = countrydata['iso3']
    try:
        dataset.add_country_location(countryiso)
    except HDXError as e:
        logger.exception('%s has a problem! %s' % (countryname, e))
        return None, None

    earliest_year = 10000
    latest_year = 0
    for endpoint in endpoints:
        url = '%s%s?iso3=%s' % (base_url, endpoint, countryiso)
        response = downloader.download(url)
        json = response.json()
        for result in json['results']:
            year = result.get('year')
            if year is None:
                continue
            if year > latest_year:
                latest_year = year
            if year < earliest_year:
                earliest_year = year

        dataset.add_update_resource(get_resource(endpoint, response.url))
    dataset.set_dataset_year_range(earliest_year, latest_year)

    showcase = Showcase({
        'name': '%s-showcase' % dataset['name'],
        'title': 'IDMC %s Summary Page' % countryname,
        'notes': 'Click the image on the right to go to the IDMC summary page for the %s dataset' % countryname,
        'url': 'http://www.internal-displacement.org/countries/%s/' % countryname,
        'image_url': 'http://www.internal-displacement.org/themes/idmc-flat/img/logo.png'
    })
    showcase.add_tags(tags)
    return dataset, showcase
