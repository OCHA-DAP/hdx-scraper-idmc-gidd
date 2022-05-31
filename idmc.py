#!/usr/bin/python
"""
IDMC:
------------

Reads IDMC HXLated csvs and creates datasets.

"""
import logging

import hxl
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_lists_add, extract_list_from_list_of_dict
from hdx.utilities.downloader import DownloadError
from hdx.utilities.text import get_matching_then_nonmatching_text
from slugify import slugify

logger = logging.getLogger(__name__)


def get_dataset(title, tags, name):
    logger.info(f"Creating dataset: {title}")
    dataset = Dataset({"name": slugify(name).lower(), "title": title})
    dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
    dataset.set_organization("647d9d8c-4cac-4c33-b639-649aad1c2893")
    dataset.set_expected_update_frequency("Every year")
    dataset.set_subnational(False)
    dataset.add_tags(tags)
    return dataset


def generate_indicator_datasets_and_showcase(downloader, folder, indicators, tags):
    datasets = dict()
    countriesdata = dict()
    headersdata = dict()
    for indicator in indicators:
        metadata = downloader.download_tabular_key_value(indicator["spreadsheet"])
        name = metadata["Indicator Name"]
        title = name
        dataset = get_dataset(title, tags, f"idmc-{name}")
        dataset[
            "notes"
        ] = f"{metadata['Long definition']}\n\nContains data from IDMC's [Global Internal Displacement Database](http://www.internal-displacement.org/database/displacement-data)."
        dataset["methodology_other"] = metadata["Statistical concept and methodology"]
        dataset["caveats"] = metadata["Limitations and exceptions"]
        dataset.add_other_location("world")
        url = indicator["url"]
        name = indicator["name"]
        path = downloader.download_file(url, folder=folder, filename=f"{name}.xlsx")
        data = hxl.data(path, allow_local=True)
        headers = data.headers
        hxltags = data.display_tags
        headersdata[name] = headers, hxltags
        years = set()
        rows = [headers, hxltags]
        for row in data:
            newrow = list()
            for hxltag in hxltags:
                newrow.append(row.get(hxltag))
            rows.append(newrow)
            iso3 = row.get("#country+code")
            epcountrydata = countriesdata.get(iso3, dict())
            dict_of_lists_add(epcountrydata, name, row)
            countriesdata[iso3] = epcountrydata
            year = row.get("#date+year")
            if year is None:
                continue
            years.add(year)

        resourcedata = {"name": name, "description": title}
        filename = f"{name}.csv"
        dataset.generate_resource_from_rows(folder, filename, rows, resourcedata)

        years = sorted(years)
        dataset.set_dataset_year_range(years[0], years[-1])
        datasets[name] = dataset

    title = "IDMC Global Report on Internal Displacement"
    slugified_name = slugify(title).lower()
    showcase = Showcase(
        {
            "name": slugified_name,
            "title": title,
            "notes": f"Click the image to go to the {title}",
            "url": "http://www.internal-displacement.org/global-report/grid2018/",
            "image_url": "http://www.internal-displacement.org/global-report/grid2018/img/ogimage.jpg",
        }
    )
    showcase.add_tags(tags)
    return datasets, showcase, headersdata, countriesdata


def generate_country_dataset_and_showcase(
    downloader, folder, headersdata, countryiso, countrydata, indicator_datasets, tags
):
    indicator_datasets_list = indicator_datasets.values()
    title = extract_list_from_list_of_dict(indicator_datasets_list, "title")
    countryname = Country.get_country_name_from_iso3(countryiso)
    dataset = get_dataset(
        f"{countryname} - {title[0]}", tags, f"IDMC IDP data for {countryname}"
    )
    try:
        dataset.add_country_location(countryiso)
    except HDXError as e:
        logger.exception(f"{countryname} has a problem! {e}")
        return None, None, None
    description = extract_list_from_list_of_dict(indicator_datasets_list, "notes")
    dataset["notes"] = get_matching_then_nonmatching_text(
        description, separator="\n\n", ignore="\n"
    )
    methodology = extract_list_from_list_of_dict(
        indicator_datasets_list, "methodology_other"
    )
    dataset["methodology_other"] = get_matching_then_nonmatching_text(methodology)
    caveats = extract_list_from_list_of_dict(indicator_datasets_list, "caveats")
    dataset["caveats"] = get_matching_then_nonmatching_text(caveats)

    years = set()
    bites_disabled = [True, True, True]
    for endpoint in countrydata:
        data = countrydata[endpoint]
        headers, hxltags = headersdata[endpoint]
        rows = [headers, hxltags]
        for row in data:
            newrow = list()
            for hxltag in hxltags:
                newrow.append(row.get(hxltag))
            rows.append(newrow)
            year = row.get("#date+year")
            conflict_stock = row.get("#affected+idps+ind+stock+conflict")
            if conflict_stock:
                bites_disabled[0] = False
            conflict_internaldisp = row.get("#affected+idps+ind+internaldisp+conflict")
            if conflict_internaldisp:
                bites_disabled[1] = False
            disaster_internaldisp = row.get("#affected+idps+ind+internaldisp+disaster")
            if disaster_internaldisp:
                bites_disabled[2] = False
            if year is None:
                continue
            years.add(year)
        name = indicator_datasets[endpoint].get_resources()[0]["description"]
        resourcedata = {
            "name": endpoint,
            "description": f"{name} for {countryname}",
        }
        filename = f"{endpoint}_{countryname}.csv"
        dataset.generate_resource_from_rows(folder, filename, rows, resourcedata)
    years = sorted(years)
    dataset.set_dataset_year_range(years[0], years[-1])
    url = f"http://www.internal-displacement.org/countries/{countryname.replace(' ', '-')}/"
    try:
        downloader.setup(url)
    except DownloadError:
        altname = Country.get_country_info_from_iso3(countryiso)[
            "#country+alt+i_en+name+v_unterm"
        ]
        url = f"http://www.internal-displacement.org/countries/{altname}/"
        try:
            downloader.setup(url)
        except DownloadError:
            return dataset, None, bites_disabled
    showcase = Showcase(
        {
            "name": f"{dataset['name']}-showcase",
            "title": f"IDMC {countryname} Summary Page",
            "notes": f"Click the image to go to the IDMC summary page for the {countryname} dataset",
            "url": url,
            "image_url": "http://www.internal-displacement.org/sites/default/files/logo_0.png",
        }
    )
    showcase.add_tags(tags)
    return dataset, showcase, bites_disabled
