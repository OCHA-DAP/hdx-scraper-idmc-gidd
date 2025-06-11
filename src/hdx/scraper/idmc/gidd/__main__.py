#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""

import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.scraper.idmc.gidd._version import __version__
from hdx.scraper.idmc.gidd.pipeline import Pipeline
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    progress_storing_folder,
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-idmc-gidd"


def main(save: bool = False, use_saved: bool = False) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """

    logger.info(f"##### {lookup} version {__version__} ####")
    configuration = Configuration.read()
    User.check_current_user_write_access(
        "647d9d8c-4cac-4c33-b639-649aad1c2893", configuration=configuration
    )
    with wheretostart_tempdir_batch(lookup) as info:
        folder = info["folder"]
        with Download(
            extra_params_yaml=join(expanduser("~"), ".extraparams.yaml"),
            extra_params_lookup=lookup,
        ) as downloader:
            retriever = Retrieve(
                downloader, folder, "saved_data", folder, save, use_saved
            )
            folder = info["folder"]
            batch = info["batch"]
            pipeline = Pipeline(configuration, retriever, folder)
            pipeline.download_indicators()
            countries = pipeline.get_countryiso3s()
            indicators = pipeline.get_indicators()
            datasets, showcase = pipeline.generate_indicator_datasets_and_showcase()

            logger.info(f"Number of indicator datasets to upload: {len(indicators)}")
            logger.info(f"Number of country datasets to upload: {len(countries)}")

            showcase_not_added = True
            for nextdict in indicators:
                if showcase_not_added:
                    showcase.create_in_hdx()
                    showcase_not_added = False
                dataset = datasets[nextdict["name"]]
                dataset.update_from_yaml(
                    script_dir_plus_file(
                        join("config", "hdx_dataset_static.yaml"), main
                    )
                )
                dataset.generate_quickcharts(
                    path=script_dir_plus_file(
                        join("config", nextdict["resourceview"]), main
                    )
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
                (
                    dataset,
                    showcase,
                    bites_disabled,
                ) = pipeline.generate_country_dataset_and_showcase(
                    countryiso,
                )
                if dataset:
                    dataset.update_from_yaml(
                        script_dir_plus_file(
                            join("config", "hdx_dataset_static.yaml"), main
                        )
                    )
                    dataset.generate_quickcharts(
                        path=script_dir_plus_file(
                            join("config", "hdx_resource_view_static.yaml"), main
                        ),
                        bites_disabled=bites_disabled,
                    )
                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        hxl_update=False,
                        updated_by_script="HDX Scraper: IDMC GIDD",
                        batch=batch,
                    )
                    resources = dataset.get_resources()
                    resource_ids = [
                        x["id"] for x in sorted(resources, key=lambda x: len(x["name"]))
                    ]
                    dataset.reorder_resources(resource_ids, hxl_update=False)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
