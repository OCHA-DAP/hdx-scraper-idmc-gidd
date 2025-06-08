#!/usr/bin/python
"""
Unit tests for IDMC GIDD.

"""

from os.path import join

import pytest

from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.scraper.idmc.gidd.idmc import IDMC
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent


class TestIDMC:
    afg_dataset = {
        "name": "idmc-idp-data-for-afghanistan",
        "title": "Afghanistan - internally displaced persons-idps",
        "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
        "owner_org": "647d9d8c-4cac-4c33-b639-649aad1c2893",
        "data_update_frequency": "365",
        "subnational": "0",
        "tags": [
            {"name": "hxl", "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87"},
            {
                "name": "displacement",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
            {
                "name": "internally displaced persons-idp",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
            {
                "name": "conflict-violence",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
        ],
        "groups": [{"name": "afg"}],
        "notes": "Description\n\nContains data from IDMC's [Global Internal Displacement Database](http://www.internal-displacement.org/database/displacement-data).",
        "methodology_other": "",
        "caveats": "",
        "dataset_date": "[2008-01-01T00:00:00 TO 2018-12-31T23:59:59]",
    }
    afg_resources = [
        {
            "description": "internally displaced persons-idps for Afghanistan",
            "format": "csv",
            "name": "displacement_data",
            "resource_type": "file.upload",
            "url_type": "upload",
        },
        {
            "description": "internally displaced persons-idps (new displacement associated with disasters) for Afghanistan",
            "format": "csv",
            "name": "disaster_data",
            "resource_type": "file.upload",
            "url_type": "upload",
        },
    ]

    @pytest.fixture(scope="function")
    def fixtures(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="function")
    def input_folder(self, fixtures):
        return join(fixtures, "input")

    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            hdx_read_only=True,
            user_agent="test",
            project_config_yaml=join("config", "project_configuration.yaml"),
        )
        UserAgent.set_global("test")
        Country.countriesdata(use_live=False)
        Locations.set_validlocations(
            [
                {"name": "afg", "title": "Afghanistan"},
                {"name": "tza", "title": "Tanzania"},
                {"name": "world", "title": "World"},
            ]
        )
        tags = (
            "hxl",
            "conflict-violence",
            "displacement",
            "internally displaced persons-idp",
            "natural disasters",
        )
        Vocabulary._tags_dict = {tag: {"Action to Take": "ok"} for tag in tags}
        tags = [{"name": tag} for tag in tags]
        Vocabulary._approved_vocabulary = {
            "tags": tags,
            "id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            "name": "approved",
        }
        return Configuration.read()

    def test_generate_datasets_and_showcase(
        self, configuration, fixtures, input_folder
    ):
        def compare(dataset, expected_dataset, expected_resources, suffix=""):
            assert dataset == expected_dataset
            resources = dataset.get_resources()
            assert resources == expected_resources
            for resource in resources:
                resource_name = resource["name"]
                filename = f"{resource_name}{suffix}.csv"
                assert_files_same(join(fixtures, filename), resource.file_to_upload)

        with temp_dir(
            "test_idmc", delete_on_success=True, delete_on_failure=False
        ) as folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, input_folder, folder, False, True
                )
                # indicator dataset test
                idmc = IDMC(configuration, retriever, folder)
                indicators = idmc.get_indicators()
                assert len(indicators) == 2
                idmc.download_indicators()
                assert len(idmc.indicator_data["displacement"]["rows"]) == 830
                assert len(idmc.indicator_data["displacement"]["rows_by_country"]) == 85
                assert len(idmc.indicator_data["disaster"]["rows"]) == 13117
                assert len(idmc.indicator_data["disaster"]["rows_by_country"]) == 205
                countries = idmc.get_countryiso3s()
                assert len(countries) == 207
                assert countries[1] == {"iso3": "AFG"}
                datasets, showcase = idmc.generate_indicator_datasets_and_showcase()
                compare(
                    datasets["displacement"],
                    {
                        "name": "idmc-internal-displacements-new-displacements-idps",
                        "title": "Internal Displacements (New Displacements) – IDPs",
                        "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                        "owner_org": "647d9d8c-4cac-4c33-b639-649aad1c2893",
                        "data_update_frequency": "365",
                        "subnational": "0",
                        "tags": [
                            {
                                "name": "hxl",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                            {
                                "name": "displacement",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                            {
                                "name": "internally displaced persons-idp",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                            {
                                "name": "conflict-violence",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                        ],
                        "notes": 'Internally displaced persons are defined according to [the 1998 Guiding Principles](https://www.internal-displacement.org/internal-displacement/guiding-principles-on-internal-displacement) as people or groups of people who have been forced or obliged to flee or to leave their homes or places of habitual residence, in particular as a result of armed conflict, or to avoid the effects of armed conflict, situations of generalized violence, violations of human rights, or natural or human-made disasters and who have not crossed an international border.\n\n\n"Internally displaced persons - IDPs" refers to the number of people living in displacement as of the end of each year.\n\n\nContains data from IDMC\'s [Global Internal Displacement Database](http://www.internal-displacement.org/database/displacement-data).\n',
                        "groups": [{"name": "world"}],
                        "dataset_date": "[2009-01-01T00:00:00 TO 2022-12-31T23:59:59]",
                    },
                    [
                        {
                            "name": "Internal Displacements (New Displacements) – IDPs",
                            "description": "Internal Displacements (New Displacements) – IDPs",
                            "format": "csv",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        }
                    ],
                )

                compare(
                    datasets["disaster"],
                    {
                        "name": "idmc-internal-displacements-new-displacements-associated-with-disasters",
                        "title": "Internal displacements (new displacements) associated with disasters",
                        "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                        "owner_org": "647d9d8c-4cac-4c33-b639-649aad1c2893",
                        "data_update_frequency": "365",
                        "subnational": "0",
                        "tags": [
                            {
                                "name": "hxl",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                            {
                                "name": "displacement",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                            {
                                "name": "internally displaced persons-idp",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                            {
                                "name": "natural disasters",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                        ],
                        "notes": 'Internally displaced persons are defined according to [the 1998 Guiding Principles](https://www.internal-displacement.org/internal-displacement/guiding-principles-on-internal-displacement) as people or groups of people who have been forced or obliged to flee or to leave their homes or places of habitual residence, in particular as a result of armed conflict, or to avoid the effects of armed conflict, situations of generalized violence, violations of human rights, or natural or human-made disasters and who have not crossed an international border.\n\n\n"Internal displacements (New Displacements)" refers to the number of new cases or incidents of displacement recorded, rather than the number of people displaced. This is done because people may have been displaced more than once.\n\n\nContains data from IDMC\'s [Global Internal Displacement Database](http://www.internal-displacement.org/database/displacement-data).\n',
                        "groups": [{"name": "world"}],
                        "dataset_date": "[2008-01-01T00:00:00 TO 2022-12-31T23:59:59]",
                    },
                    [
                        {
                            "name": "Internal displacements (new displacements) associated with disasters",
                            "description": "Internal displacements (new displacements) associated with disasters",
                            "format": "csv",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        }
                    ],
                )
                assert showcase == {
                    "name": "idmc-global-report-on-internal-displacement",
                    "title": "IDMC Global Report on Internal Displacement",
                    "notes": "Click the image to go to the IDMC Global Report on Internal Displacement",
                    "url": "https://www.internal-displacement.org/global-report/grid2024/",
                    "image_url": "https://api.internal-displacement.org/sites/default/files/publications/images/idmc-2024-grid-cover-image.jpg",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "displacement",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "internally displaced persons-idp",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "conflict-violence",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "natural disasters",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                    ],
                }

                (
                    dataset,
                    showcase,
                    bites_disabled,
                ) = idmc.generate_country_dataset_and_showcase(
                    "AFG",
                )
                compare(
                    dataset,
                    {
                        "name": "idmc-idp-data-afg",
                        "title": "Afghanistan - Internal Displacements (New Displacements) – IDPs",
                        "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                        "owner_org": "647d9d8c-4cac-4c33-b639-649aad1c2893",
                        "data_update_frequency": "365",
                        "subnational": "0",
                        "tags": [
                            {
                                "name": "hxl",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                            {
                                "name": "displacement",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                            {
                                "name": "internally displaced persons-idp",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                            {
                                "name": "conflict-violence",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                            {
                                "name": "natural disasters",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                        ],
                        "groups": [{"name": "afg"}],
                        "notes": 'Internally displaced persons are defined according to [the 1998 Guiding Principles](https://www.internal-displacement.org/internal-displacement/guiding-principles-on-internal-displacement) as people or groups of people who have been forced or obliged to flee or to leave their homes or places of habitual residence, in particular as a result of armed conflict, or to avoid the effects of armed conflict, situations of generalized violence, violations of human rights, or natural or human-made disasters and who have not crossed an international border.\n\n\n"Internally displaced persons - IDPs" refers to the number of people living in displacement as of the end of each year.\n\n\n"Internal displacements (New Displacements)" refers to the number of new cases or incidents of displacement recorded, rather than the number of people displaced. This is done because people may have been displaced more than once.\n\n\nContains data from IDMC\'s [Global Internal Displacement Database](http://www.internal-displacement.org/database/displacement-data).\n',
                        "dataset_date": "[2008-01-01T00:00:00 TO 2022-12-31T23:59:59]",
                    },
                    [
                        {
                            "name": "Internal Displacements (New Displacements) – IDPs",
                            "description": "Internal Displacements (New Displacements) – IDPs for Afghanistan",
                            "format": "csv",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "name": "Internal displacements (new displacements) associated with disasters",
                            "description": "Internal displacements (new displacements) associated with disasters for Afghanistan",
                            "format": "csv",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                    ],
                    suffix="_AFG",
                )
                assert showcase == {
                    "name": "idmc-idp-data-afg-showcase",
                    "title": "IDMC Afghanistan Summary Page",
                    "notes": "Click the image to go to the IDMC summary page for the Afghanistan dataset",
                    "url": "http://www.internal-displacement.org/countries/Afghanistan/",
                    "image_url": "https://www.internal-displacement.org/sites/default/files/logo_0.png",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "displacement",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "internally displaced persons-idp",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "conflict-violence",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "natural disasters",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                    ],
                }
                assert bites_disabled == [False, False]
