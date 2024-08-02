import json
import os
import pytest
import re
from unittest import mock

import pandas as pd
import synapseclient

from scripts.table_updates import (
    update_data_table,
)


@pytest.fixture
def mock_syn():
    return mock.create_autospec(synapseclient.Synapse)


@pytest.fixture
def mock_synapse(mock_syn):
    # returns the mocked syn and release table synid
    mock_syn = mock.MagicMock()
    release_files_table_synid = "syn12345"
    # Mock the tableQuery result
    mock_syn.tableQuery.return_value.asDataFrame.return_value = pd.DataFrame(
        {
            "release": ["v1.0", "v2.0"],
            "fileSynId": ["syn23456", "syn34567"],
            "name": ["data_clinical_sample.txt", "data_clinical_patient.txt"],
        }
    )
    # Mock the Link entity retrieval
    clinical_link_ent_mock = {
        "linksTo": {"targetId": "syn88888", "targetVersionNumber": 3}
    }
    mock_syn.get.return_value = mock.MagicMock(path="path/to/clinical_file.csv")
    return mock_syn, release_files_table_synid


@pytest.fixture
def mock_release_version():
    return "v1.0"


@pytest.fixture
def config():
    yield {
        "primary":{
            "NSCLC": "syn23285494",
            "CRC": "syn23285418",
            "BrCa": "syn23286608",
            "PANC": "syn24175803",
            "Prostate": "syn25610393",
            "BLADDER": "syn26721150",
            "NSCLC2": "syn51318735",
            "CRC2": "syn52943208",
            "RENAL": "syn59474241"
        },
        "irr":{
            "BrCa": "syn24241519",
            "PANC": "syn25610271",
            "Prostate": "syn26275497",
            "BLADDER": "syn26721151",
            "NSCLC2": "syn51318736",
            "CRC2": "syn52943210",
            "RENAL": "syn59474249"
        },
        "main_genie_release_version": "16.6-consortium",
        "main_genie_data_release_files": "syn16804261",
        "main_genie_sample_mapping_table": "syn7434273"
    }



def test_get_main_genie_clinical_sample_file_success(
    mock_synapse, mock_release_version
):
    mock_syn, mock_release_files_table_synid = mock_synapse
    # Mock pandas.read_csv to return a non-empty DataFrame
    clinical_df_mock = pd.DataFrame(
        {"SAMPLE_ID": [1, 2, 3], "SEQ_YEAR": [2014, 2014, 2013], "OTHER_ID": [6, 7, 8]}
    )
    pd.read_csv = mock.MagicMock(return_value=clinical_df_mock)

    # Call the function
    update_data_table.get_main_genie_clinical_sample_file(
        mock_syn, mock_release_version, mock_release_files_table_synid
    )
    mock_syn.tableQuery.assert_called_once_with(
        f"SELECT * FROM {mock_release_files_table_synid}"
    )
    # Assert that syn.get was called in order
    mock_syn.get.assert_called_with("syn23456", followLink=True)
    pd.read_csv.assert_called_once_with(
        "path/to/clinical_file.csv", sep="\t", skiprows=4
    )


def test_get_main_genie_clinical_sample_file_empty_file(
    mock_synapse, mock_release_version
):
    mock_syn, mock_release_files_table_synid = mock_synapse

    # Mock pandas.read_csv to return an empty DataFrame
    pd.read_csv = mock.MagicMock(return_value=pd.DataFrame())

    # Call the function and assert the assertion error is raised
    with pytest.raises(
        AssertionError, match="Clinical file pulled from syn23456 link is empty."
    ):
        update_data_table.get_main_genie_clinical_sample_file(
            mock_syn, mock_release_version, mock_release_files_table_synid
        )

    mock_syn.tableQuery.assert_called_once_with(
        f"SELECT * FROM {mock_release_files_table_synid}"
    )
    # Assert that syn.get was called in order
    mock_syn.get.assert_called_with("syn23456", followLink=True)
    pd.read_csv.assert_called_once_with(
        "path/to/clinical_file.csv", sep="\t", skiprows=4
    )


def test_get_main_genie_clinical_sample_file_no_req_cols(
    mock_synapse, mock_release_version
):
    mock_syn, mock_release_files_table_synid = mock_synapse

    # Mock pandas.read_csv to return an empty DataFrame
    pd.read_csv = mock.MagicMock(return_value=pd.DataFrame({"col1": [1, 2, 3]}))

    # Call the function and assert the assertion error is raised
    with pytest.raises(
        AssertionError,
        match=re.escape(
            "Clinical file pulled from syn23456 link is missing an expected column. "
            "Expected columns: ['SAMPLE_ID', 'SEQ_YEAR']"
        ),
    ):
        update_data_table.get_main_genie_clinical_sample_file(
            mock_syn, mock_release_version, mock_release_files_table_synid
        )

    mock_syn.tableQuery.assert_called_once_with(
        f"SELECT * FROM {mock_release_files_table_synid}"
    )
    # Assert that syn.get was called in order
    mock_syn.get.assert_called_with("syn23456", followLink=True)
    pd.read_csv.assert_called_once_with(
        "path/to/clinical_file.csv", sep="\t", skiprows=4
    )


@pytest.mark.skip(reason="This test is skipped because this integration test doesn't work in pytest env")
def test_get_main_genie_clinical_sample_file_integration_test(config):
    syn = synapseclient.login()

    update_data_table.get_main_genie_clinical_sample_file(
        syn,
        release=config["main_genie_release_version"],
        release_files_table_synid=config["main_genie_data_release_files"],
    )
