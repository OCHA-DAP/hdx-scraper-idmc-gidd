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
from hdx.utilities.dictandlist import extract_list_from_list_of_dict
from hdx.utilities.text import get_matching_then_nonmatching_text, get_matching_text
from slugify import slugify

logger = logging.getLogger(__name__)


def get_countriesdata(base_url, downloader):
    response = downloader.download('%scountries' % base_url)
    jsonresponse = response.json()
    return jsonresponse['results']


def get_resource(endpoint, description, url):
    resource = {
        'name': endpoint,
        'format': 'json',
        'url': url,
        'description': description
    }
    return resource


def get_dataset(title, tags, name=None):
    logger.info('Creating dataset: %s' % title)
    if name is None:
        slugified_name = slugify(title).lower()
    else:
        slugified_name = slugify(name).lower()
    dataset = Dataset({
        'name': slugified_name,
        'title': title
    })
    dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    dataset.set_organization('647d9d8c-4cac-4c33-b639-649aad1c2893')
    dataset.set_expected_update_frequency('Every year')
    dataset.set_subnational(False)
    tags = ['population', 'displacement', 'idmc']
    dataset.add_tags(tags)
    return dataset


def generate_indicator_datasets_and_showcase(base_url, downloader, endpoints, tags):
    datasets = dict()
    for endpoint in endpoints.keys():
        metadata = downloader.download_tabular_key_value(endpoints[endpoint])
        name = metadata['Indicator Name']
        title = name
        dataset = get_dataset(title, tags)
        dataset['notes'] = "%s\n\nContains data from IDMC's [data portal](https://github.com/idmc-labs/IDMC-Platform-API/wiki)." % metadata['Long definition']
        dataset['methodology_other'] = metadata['Statistical concept and methodology']
        dataset['caveats'] = metadata['Limitations and exceptions']
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

        dataset.add_update_resource(get_resource(endpoint, name, response.url))
        dataset.set_dataset_year_range(earliest_year, latest_year)
        datasets[endpoint] = dataset

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


def generate_country_dataset_and_showcase(base_url, downloader, indicator_datasets, countrydata, endpoints, tags):
    countryname = countrydata['geo_name']
    indicator_datasets_list = indicator_datasets.values()
    title = extract_list_from_list_of_dict(indicator_datasets_list, 'title')
    dataset = get_dataset('%s - %s' % (countryname, get_matching_text(title, end_characters=' ').strip()), tags,
                          'IDMC IDP data for %s' % countryname)
    countryiso = countrydata['iso3']
    try:
        dataset.add_country_location(countryiso)
    except HDXError as e:
        logger.exception('%s has a problem! %s' % (countryname, e))
        return None, None
    description = extract_list_from_list_of_dict(indicator_datasets_list, 'notes')
    dataset['notes'] = get_matching_then_nonmatching_text(description, separator='\n\n', ignore='\n')
    methodology = extract_list_from_list_of_dict(indicator_datasets_list, 'methodology_other')
    dataset['methodology_other'] = get_matching_then_nonmatching_text(methodology)
    caveats = extract_list_from_list_of_dict(indicator_datasets_list, 'caveats')
    dataset['caveats'] = get_matching_then_nonmatching_text(caveats)

    earliest_year = 10000
    latest_year = 0
    for endpoint in endpoints.keys():
        url = '%s%s?iso3=%s' % (base_url, endpoint, countryiso)
        response = downloader.download(url)
        json = response.json()
        results = json['results']
        if results is None:
            continue
        for result in results:
            year = result.get('year')
            if year is None:
                continue
            if year > latest_year:
                latest_year = year
            if year < earliest_year:
                earliest_year = year

        dataset.add_update_resource(get_resource(endpoint,
                                                 indicator_datasets[endpoint].get_resources()[0]['description'],
                                                 response.url))
    dataset.set_dataset_year_range(earliest_year, latest_year)

    showcase = Showcase({
        'name': '%s-showcase' % dataset['name'],
        'title': 'IDMC %s Summary Page' % countryname,
        'notes': 'Click the image on the right to go to the IDMC summary page for the %s dataset' % countryname,
        'url': 'http://www.internal-displacement.org/countries/%s/' % countryname.replace(' ', '-'),
        'image_url': 'http://www.internal-displacement.org/themes/idmc-flat/img/logo.png'
    })
    showcase.add_tags(tags)
    return dataset, showcase
