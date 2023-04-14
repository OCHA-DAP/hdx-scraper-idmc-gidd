#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.facades.simple import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_folder, wheretostart_tempdir_batch
from idmc import (
    generate_country_dataset_and_showcase,
    generate_indicator_datasets_and_showcase,
)

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-idmc"


def main():
    """Generate dataset and create it in HDX"""

    with Download() as downloader:
        with wheretostart_tempdir_batch(lookup) as info:
            folder = info["folder"]
            batch = info["batch"]
            indicators = Configuration.read()["indicators"]
            tags = Configuration.read()["tags"]
            (
                datasets,
                showcase,
                headersdata,
                countriesdata,
            ) = generate_indicator_datasets_and_showcase(
                downloader, folder, indicators, tags
            )
            countries = [{"iso3": x} for x in sorted(countriesdata)]

            logger.info(f"Number of indicator datasets to upload: {len(indicators)}")
            logger.info(f"Number of country datasets to upload: {len(countries)}")

            showcase_not_added = True
            for _, nextdict in progress_storing_folder(info, indicators, "name"):
                if showcase_not_added:
                    showcase.create_in_hdx()
                    showcase_not_added = False
                dataset = datasets[nextdict["name"]]
                dataset.update_from_yaml()
                dataset.generate_quickcharts(
                    path=join("config", nextdict["resourceview"])
                )
                dataset.create_in_hdx(
                    remove_additional_resources=True,
                    hxl_update=False,
                    updated_by_script="HDX Scraper: IDMC",
                    batch=batch,
                )
                showcase.add_dataset(dataset)

            for _, nextdict in progress_storing_folder(info, countries, "iso3"):
                countryiso = nextdict["iso3"]
                countrydata = countriesdata[countryiso]
                (
                    dataset,
                    showcase,
                    bites_disabled,
                ) = generate_country_dataset_and_showcase(
                    downloader,
                    folder,
                    headersdata,
                    countryiso,
                    countrydata,
                    datasets,
                    tags,
                )
                if dataset:
                    dataset.update_from_yaml()
                    dataset.generate_quickcharts(bites_disabled=bites_disabled)
                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        hxl_update=False,
                        updated_by_script="HDX Scraper: IDMC",
                        batch=batch,
                    )
                    resources = dataset.get_resources()
                    resource_ids = [
                        x["id"]
                        for x in sorted(
                            resources, key=lambda x: len(x["name"]), reverse=True
                        )
                    ]
                    dataset.reorder_resources(resource_ids, hxl_update=False)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
    )
