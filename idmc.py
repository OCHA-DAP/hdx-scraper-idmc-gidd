#!/usr/bin/python
"""
IDMC:
------------

Reads IDMC HXLated csvs and creates datasets.

"""
import logging

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_lists_add, extract_list_from_list_of_dict
from hdx.utilities.downloader import DownloadError
from hdx.utilities.text import get_matching_then_nonmatching_text
from slugify import slugify

logger = logging.getLogger(__name__)


class IDMC:
    def __init__(self, configuration, retriever, folder):
        self.configuration = configuration
        self.retriever = retriever
        self.folder = folder
        self.countries = set()
        self.indicator_data = {}
        self.countrymapping = {}

    @staticmethod
    def get_dataset(title, tags, name):
        logger.info(f"Creating dataset: {title}")
        dataset = Dataset({"name": slugify(name).lower(), "title": title})
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("647d9d8c-4cac-4c33-b639-649aad1c2893")
        dataset.set_expected_update_frequency("Every year")
        dataset.set_subnational(False)
        dataset.add_tags(tags)
        return dataset

    def download_data(self, url, basename):
        rows = []
        rows_by_country = {}
        i = 0
        while url:
            filename = f"{basename}_{i}.json"
            json = self.retriever.download_json(url, filename=filename)
            for row in json["results"]:
                countryiso = row["iso3"]
                self.countries.add(countryiso)
                self.countrymapping[countryiso] = row["country_name"]
                row["country_name"] = Country.get_country_name_from_iso3(countryiso)
                rows.append(row)
                dict_of_lists_add(rows_by_country, countryiso, row)
            url = json["next"]
            i += 1
        return rows, rows_by_country

    def download_indicators(self):
        indicators = self.configuration["indicators"]
        for indicator in indicators:
            url = indicator["url"]
            name = indicator["name"]
            rows, rows_by_country = self.download_data(url, name)
            self.indicator_data[name] = {"rows": rows, "rows_by_country": rows_by_country}

    def get_countryiso3s(self):
        return [{"iso3": countryiso} for countryiso in sorted(self.countries)]

    def get_indicators(self):
        return self.configuration["indicators"]

    def generate_indicator_datasets_and_showcase(self):
        tags = self.configuration["tags"]
        datasets = dict()
        for indicator in self.get_indicators():
            metadata = self.retriever.downloader.download_tabular_key_value(indicator["spreadsheet"])
            name = metadata["Indicator Name"]
            title = name
            dataset = self.get_dataset(title, tags, f"idmc-{name}")
            dataset[
                "notes"
            ] = f"{metadata['Long definition']}\n\nContains data from IDMC's [Global Internal Displacement Database](http://www.internal-displacement.org/database/displacement-data)."
            dataset["methodology_other"] = metadata["Statistical concept and methodology"]
            dataset["caveats"] = metadata["Limitations and exceptions"]
            dataset.add_other_location("world")
            key = indicator["name"]
            rows = self.indicator_data[key]["rows"]
            years = set()
            for row in rows:
                year = row["year"]
                years.add(year)
                for header in indicator["flatten"]:
                    row[header] = ",".join(row[header])
            rows.insert(0, indicator["hxltags"])
            resourcedata = {"name": name, "description": title}
            filename = f"{name}.csv"
            dataset.generate_resource_from_rows(self.folder, filename, rows, resourcedata)

            years = sorted(years)
            dataset.set_reference_period_year_range(years[0], years[-1])
            datasets[key] = dataset

        title = "IDMC Global Report on Internal Displacement"
        slugified_name = slugify(title).lower()
        showcase = Showcase(
            {
                "name": slugified_name,
                "title": title,
                "notes": f"Click the image to go to the {title}",
                "url": "https://www.internal-displacement.org/global-report/grid2023/",
                "image_url": "https://www.internal-displacement.org/sites/default/files/styles/optimized/public/2023_Internal_Displacement_Monitoring_Centre_IDMC_2023_GRID_Preview.png?itok=apWZ2Y_K",
            }
        )
        showcase.add_tags(tags)
        return datasets, showcase


    def generate_country_dataset_and_showcase(self,
        countryiso, indicator_datasets
    ):
        tags = self.configuration["tags"]
        indicator_datasets_list = indicator_datasets.values()
        title = extract_list_from_list_of_dict(indicator_datasets_list, "title")
        countryname = Country.get_country_name_from_iso3(countryiso)
        dataset = self.get_dataset(
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
        bites_disabled = [True, True]
        for indicator in self.get_indicators():
            key = indicator["name"]
            rows = self.indicator_data[key]["rows_by_country"][countryiso]
            for row in rows:
                year = row["year"]
                years.add(year)
                total_displacement = row["total_displacement"]
                if total_displacement:
                    bites_disabled[0] = False
                new_displacement = row["new_displacement"]
                if new_displacement:
                    bites_disabled[1] = False
            rows.insert(0, indicator["hxltags"])
            metadata = self.retriever.downloader.download_tabular_key_value(indicator["spreadsheet"])
            name = metadata["Indicator Name"]
            resourcedata = {
                "name": name,
                "description": f"{name} for {countryname}",
            }
            filename = f"{key}_{countryname}.csv"
            dataset.generate_resource_from_rows(self.folder, filename, rows, resourcedata)
        years = sorted(years)
        dataset.set_reference_period_year_range(years[0], years[-1])
        internal_countryname = self.countrymapping[countryiso]
        url = f"http://www.internal-displacement.org/countries/{internal_countryname.replace(' ', '-')}/"
        try:
            self.retriever.downloader.setup(url)
        except DownloadError:
            return dataset, None, bites_disabled
        showcase = Showcase(
            {
                "name": f"{dataset['name']}-showcase",
                "title": f"IDMC {countryname} Summary Page",
                "notes": f"Click the image to go to the IDMC summary page for the {countryname} dataset",
                "url": url,
                "image_url": "https://www.internal-displacement.org/sites/default/files/logo_0.png",
            }
        )
        showcase.add_tags(tags)
        return dataset, showcase, bites_disabled
