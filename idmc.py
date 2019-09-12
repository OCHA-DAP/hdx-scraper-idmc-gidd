#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
IDMC:
------------

Reads IDMC HXLated csvs and creates datasets.

"""
import json
import logging
from os.path import join

import hxl
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.resource import Resource
from hdx.data.resource_view import ResourceView
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dictandlist import extract_list_from_list_of_dict, dict_of_lists_add, write_list_to_csv
from hdx.utilities.downloader import DownloadError
from hdx.utilities.text import get_matching_then_nonmatching_text
from slugify import slugify

logger = logging.getLogger(__name__)
quickchart_resourceno = 0
extension = 'csv'


def get_resource(endpoint, description):
    resource = {
        'name': endpoint,
        'format': extension,
        'description': description
    }
    return Resource(resource)


def get_dataset(title, tags, name):
    logger.info('Creating dataset: %s' % title)
    dataset = Dataset({
        'name': slugify(name).lower(),
        'title': title
    })
    dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    dataset.set_organization('647d9d8c-4cac-4c33-b639-649aad1c2893')
    dataset.set_expected_update_frequency('Every year')
    dataset.set_subnational(False)
    dataset.add_tags(tags)
    return dataset


def generate_indicator_datasets_and_showcase(displacement_url, disaster_url, downloader, folder, endpoints, tags):
    datasets = dict()
    countriesdata = dict()
    headersdata = dict()
    for endpoint in endpoints:
        metadata = downloader.download_tabular_key_value(endpoints[endpoint])
        name = metadata['Indicator Name']
        title = name
        dataset = get_dataset(title, tags, 'idmc-%s' % name)
        dataset['notes'] = "%s\n\nContains data from IDMC's [Global Internal Displacement Database](http://www.internal-displacement.org/database/displacement-data)." % metadata['Long definition']
        dataset['methodology_other'] = metadata['Statistical concept and methodology']
        dataset['caveats'] = metadata['Limitations and exceptions']
        dataset.add_other_location('world')
        if endpoint == 'disaster_data':
            url = disaster_url
        elif endpoint == 'displacement_data':
            url = displacement_url
        else:
            raise ValueError('Invalid endpoint %s!' % endpoint)
        path = downloader.download_file(url, folder, '%s.xlsx' % endpoint)
        data = hxl.data(path, allow_local=True)
        headers = data.headers
        hxltags = data.display_tags
        headersdata[endpoint] = headers, hxltags
        earliest_year = 10000
        latest_year = 0
        rows = [headers, hxltags]
        for row in data:
            newrow = list()
            for hxltag in hxltags:
                newrow.append(row.get(hxltag))
            rows.append(newrow)
            iso3 = row.get('#country+code')
            epcountrydata = countriesdata.get(iso3, dict())
            dict_of_lists_add(epcountrydata, endpoint, row)
            countriesdata[iso3] = epcountrydata
            year = row.get('#date+year')
            if year is None:
                continue
            if year > latest_year:
                latest_year = year
            if year < earliest_year:
                earliest_year = year

        resource = get_resource(endpoint, name)
        path = join(folder, '%s.%s' % (endpoint, extension))
        write_list_to_csv(rows, path)
        resource.set_file_to_upload(path)
        dataset.add_update_resource(resource)
        dataset.set_dataset_year_range(earliest_year, latest_year)
        dataset.set_quickchart_resource(quickchart_resourceno)
        datasets[endpoint] = dataset

    title = 'IDMC Global Report on Internal Displacement'
    slugified_name = slugify(title).lower()
    showcase = Showcase({
        'name': slugified_name,
        'title': title,
        'notes': 'Click the image on the right to go to the %s' % title,
        'url': 'http://www.internal-displacement.org/global-report/grid2018/',
        'image_url': 'http://www.internal-displacement.org/global-report/grid2018/img/ogimage.jpg'
    })
    showcase.add_tags(tags)
    return datasets, showcase, headersdata, countriesdata


def generate_country_dataset_and_showcase(downloader, folder, headersdata, countryiso, countrydata, indicator_datasets, tags):
    indicator_datasets_list = indicator_datasets.values()
    title = extract_list_from_list_of_dict(indicator_datasets_list, 'title')
    countryname = Country.get_country_name_from_iso3(countryiso)
    dataset = get_dataset('%s - %s' % (countryname, title[0]), tags,
                          'IDMC IDP data for %s' % countryname)
    try:
        dataset.add_country_location(countryiso)
    except HDXError as e:
        logger.exception('%s has a problem! %s' % (countryname, e))
        return None, None, None
    description = extract_list_from_list_of_dict(indicator_datasets_list, 'notes')
    dataset['notes'] = get_matching_then_nonmatching_text(description, separator='\n\n', ignore='\n')
    methodology = extract_list_from_list_of_dict(indicator_datasets_list, 'methodology_other')
    dataset['methodology_other'] = get_matching_then_nonmatching_text(methodology)
    caveats = extract_list_from_list_of_dict(indicator_datasets_list, 'caveats')
    dataset['caveats'] = get_matching_then_nonmatching_text(caveats)

    earliest_year = 10000
    latest_year = 0
    empty_col = [True, True, True]
    for endpoint in countrydata:
        data = countrydata[endpoint]
        earliest_year = 10000
        latest_year = 0
        headers, hxltags = headersdata[endpoint]
        rows = [headers, hxltags]
        for row in data:
            newrow = list()
            for hxltag in hxltags:
                newrow.append(row.get(hxltag))
            rows.append(newrow)
            year = row.get('#date+year')
            conflict_stock = row.get('#affected+idps+ind+stock+conflict')
            if conflict_stock:
                empty_col[0] = False
            conflict_new = row.get('#affected+idps+ind+newdisp+conflict')
            if conflict_new:
                empty_col[1] = False
            disaster_new = row.get('#affected+idps+ind+newdisp+disaster')
            if disaster_new:
                empty_col[2] = False
            if year is None:
                continue
            if year > latest_year:
                latest_year = year
            if year < earliest_year:
                earliest_year = year
        name = indicator_datasets[endpoint].get_resources()[0]['description']
        resource = get_resource(endpoint, '%s for %s' % (name, countryname))
        path = join(folder, '%s_%s.%s' % (endpoint, countryname, extension))
        write_list_to_csv(rows, path)
        resource.set_file_to_upload(path)
        dataset.add_update_resource(resource)
    dataset.set_quickchart_resource(quickchart_resourceno)
    dataset.set_dataset_year_range(earliest_year, latest_year)
    url = 'http://www.internal-displacement.org/countries/%s/' % countryname.replace(' ', '-')
    try:
        downloader.setup(url)
    except DownloadError:
        altname = Country.get_country_info_from_iso3(countryiso)['#country+alt+i_en+name+v_unterm']
        url = 'http://www.internal-displacement.org/countries/%s/' % altname
        try:
            downloader.setup(url)
        except DownloadError:
            return dataset, None, empty_col
    showcase = Showcase({
        'name': '%s-showcase' % dataset['name'],
        'title': 'IDMC %s Summary Page' % countryname,
        'notes': 'Click the image on the right to go to the IDMC summary page for the %s dataset' % countryname,
        'url': url,
        'image_url': 'http://www.internal-displacement.org/sites/default/files/logo_0.png'
    })
    showcase.add_tags(tags)
    return dataset, showcase, empty_col


def generate_resource_view(dataset, path=None, empty_col=None):
    resourceview = ResourceView({'resource_id': dataset.get_resource(quickchart_resourceno)['id']})
    if path:
        resourceview.update_from_yaml(path=path)
    else:
        resourceview.update_from_yaml()
    if empty_col is not None:
        hxl_preview_config = json.loads(resourceview['hxl_preview_config'])
        for i, empty in reversed(list(enumerate(empty_col))):
            if empty:
                del hxl_preview_config['bites'][i]
        resourceview['hxl_preview_config'] = json.dumps(hxl_preview_config)

    return resourceview
