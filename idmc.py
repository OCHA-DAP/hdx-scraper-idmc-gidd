#!/usr/bin/python
"""
IDMC:
------------

Reads IDMC HXLated csvs and creates datasets.

"""
import logging
from copy import copy
from operator import itemgetter

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.downloader import DownloadError
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
    def get_dataset(title, name):
        logger.info(f"Creating dataset: {title}")
        dataset = Dataset({"name": slugify(name).lower(), "title": title})
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("647d9d8c-4cac-4c33-b639-649aad1c2893")
        dataset.set_expected_update_frequency("Every year")
        dataset.set_subnational(False)
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
                for key in row:
                    if row[key] is None:
                        row[key] = ""
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
        orig_tags = self.configuration["tags"]
        tags = copy(orig_tags)
        notes_lookup = self.configuration["notes"]
        first_part = notes_lookup["first_part"]
        last_part = notes_lookup["last_part"]
        datasets = dict()
        for indicator in self.get_indicators():
            name = indicator["title"]
            title = name
            dataset = self.get_dataset(title, f"idmc-{name}")
            key = indicator["name"]
            notes = f"{first_part}\n\n{notes_lookup[key]}\n\n{last_part}"
            dataset["notes"] = notes
            dataset.add_other_location("world")
            rows = self.indicator_data[key]["rows"]
            rows = sorted(rows, key=itemgetter(*indicator["sort"]))
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
            indicator_tags = indicator["tags"]
            dataset.add_tags(orig_tags + indicator_tags)
            tags += indicator_tags
            years = sorted(years)
            dataset.set_time_period_year_range(years[0], years[-1])
            datasets[key] = dataset

        title = "IDMC Global Report on Internal Displacement"
        slugified_name = slugify(title).lower()
        showcase = Showcase(
            {
                "name": slugified_name,
                "title": title,
                "notes": f"Click the image to go to the {title}",
                "url": "https://www.internal-displacement.org/global-report/grid2024/",
                "image_url": "https://www.internal-displacement.org/sites/default/files/styles/optimized/public/2023_Internal_Displacement_Monitoring_Centre_IDMC_2023_GRID_Preview.png?itok=apWZ2Y_K",
            }
        )
        showcase.add_tags(tags)
        return datasets, showcase

    def generate_country_dataset_and_showcase(self,
        countryiso
    ):
        tags = copy(self.configuration["tags"])
        country_dataset = self.configuration["country_dataset"]
        countryname = Country.get_country_name_from_iso3(countryiso)
        name = country_dataset["name"]
        title = country_dataset["title"]
        dataset = self.get_dataset(
            f"{countryname} - {title}", f"{name}{countryiso}"
        )
        try:
            dataset.add_country_location(countryiso)
        except HDXError as e:
            logger.exception(f"{countryname} has a problem! {e}")
            return None, None, None
        dataset["notes"] = "\n\n".join(self.configuration["notes"].values())
        years = set()
        bites_disabled = [True, True]
        for indicator in self.get_indicators():
            name = indicator["title"]
            key = indicator["name"]
            rows = self.indicator_data[key]["rows_by_country"].get(countryiso)
            if not rows:
                continue
            rows = sorted(rows, key=itemgetter(*indicator["sort"]))
            for row in rows:
                year = row["year"]
                years.add(year)
                if key == country_dataset["quickcharts"]:
                    total_displacement = row["total_displacement"]
                    if total_displacement:
                        bites_disabled[0] = False
                    new_displacement = row["new_displacement"]
                    if new_displacement:
                        bites_disabled[1] = False
            rows.insert(0, indicator["hxltags"])
            resourcedata = {
                "name": name,
                "description": f"{name} for {countryname}",
            }
            filename = f"{name}_{countryiso}.csv"
            dataset.generate_resource_from_rows(self.folder, filename, rows, resourcedata)
            tags += indicator["tags"]
        dataset.add_tags(tags)
        years = sorted(years)
        dataset.set_time_period_year_range(years[0], years[-1])
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
