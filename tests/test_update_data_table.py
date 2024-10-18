import json
import logging
import os
import re
from unittest import mock

import pandas as pd
import pytest
import synapseclient
from scripts.table_updates import utilities
from scripts.table_updates.update_data_table import *


@pytest.fixture
def mock_syn():
    return mock.create_autospec(synapseclient.Synapse)


@pytest.fixture
def mock_synapse(mock_syn):
    # returns the mocked syn and release table synid
    mock_syn = mock.MagicMock()
    release_files_table_synid = "syn12345"
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
@pytest.fixture
def column_mapping_table():
    return pd.DataFrame(
        {"genie_element": ["ETHNICITY_DETAILED", "PRIMARY_RACE_DETAILED", "SEQ_YEAR"], 
        "prissmm_element": ["naaccr_ethnicity_code", "naaccr_race_code_primary", "cpt_seq_date"],
        "prissmm_form": ["patient_characteristics", "patient_characteristics", "cancer_panel_test"]}
        )

@pytest.fixture
def release_files_df():
    return pd.DataFrame(
        {
            "release": ["v1.0", "v1.0"],
            "fileSynId": ["syn23456", "syn34567"],
            "name": ["data_clinical_sample.txt", "data_clinical_patient.txt"],
        }
        )

@pytest.mark.parametrize(
    "clinical_df_mock,form,clinical_link_synid",
    [
        (pd.DataFrame({"PATIENT_ID": [1, 2, 3], "ETHNICITY_DETAILED": [1, 2, 3],"PRIMARY_RACE_DETAILED":[1, 2, 3] ,"OTHER_ID": [6, 7, 8]}), "patient_characteristics", "syn34567"),
        (pd.DataFrame({"SAMPLE_ID": [1, 2, 3], "SEQ_YEAR": [2014, 2014, 2013], "OTHER_ID": [6, 7, 8]}), "cancer_panel_test", "syn23456"),
    ],
    ids=["get_patient_file", "get_sample_file"]
)
def test_get_main_genie_clinical_file_success(
    mock_synapse, mock_release_version, release_files_df, column_mapping_table, clinical_df_mock, form, clinical_link_synid
):
    with mock.patch.object(utilities, "download_synapse_table", return_value = release_files_df) as mock_download_synapse_table:

        mock_syn, mock_release_files_table_synid = mock_synapse
        mock_logger = mock.MagicMock(spec=logging.Logger)
        pd.read_csv = mock.MagicMock(return_value=clinical_df_mock)
  
        # Call the function
        results = get_main_genie_clinical_file(
               mock_syn, mock_release_version, mock_release_files_table_synid, form = form, column_mapping_table =column_mapping_table, logger = mock_logger
            )
        # validate
        mock_download_synapse_table.assert_called_with(mock_syn, mock_release_files_table_synid)
        mock_syn.get.assert_called_with(clinical_link_synid, followLink=True)
        pd.read_csv.assert_called_once_with(
            "path/to/clinical_file.csv", sep="\t", skiprows=4
        )
        pd.testing.assert_frame_equal(results, clinical_df_mock)
        mock_logger.info.assert_any_call(f"CLINICAL_FILE_LINK:{clinical_link_synid}")
        mock_logger.info.assert_any_call(f"RELEASE_FILES_TABLE_SYNID:{mock_release_files_table_synid}")


@pytest.mark.parametrize(
    "form,clinical_link_synid",
    [
        ("patient_characteristics", "syn34567"),
        ("cancer_panel_test", "syn23456"),
    ],
    ids=["get_patient_file", "get_sample_file"]
)
def test_get_main_genie_clinical_file_empty_file(
    mock_synapse, mock_release_version, release_files_df, column_mapping_table, form, clinical_link_synid
):
    with mock.patch.object(utilities, "download_synapse_table", return_value = release_files_df) as mock_download_synapse_table, pytest.raises(AssertionError, match=f"Clinical file pulled from {clinical_link_synid} link is empty."):
        # Mock pandas.read_csv to return an empty DataFrame
        mock_syn, mock_release_files_table_synid = mock_synapse
        mock_logger = mock.MagicMock(spec=logging.Logger)
        pd.read_csv = mock.MagicMock(return_value=pd.DataFrame())

        # Call the function and assert the assertion error is raised
        results = get_main_genie_clinical_file(
               mock_syn, mock_release_version, mock_release_files_table_synid, form = form, column_mapping_table =column_mapping_table, logger = mock_logger
            )

        # validate
        mock_download_synapse_table.assert_called_with(mock_syn, mock_release_files_table_synid)
        mock_syn.get.assert_called_with(clinical_link_synid, followLink=True)
        pd.read_csv.assert_called_once_with(
            "path/to/clinical_file.csv", sep="\t", skiprows=4
        )
        pd.testing.assert_frame_equal(results, pd.DataFrame())
        mock_logger.info.assert_any_call(f"CLINICAL_FILE_LINK:{clinical_link_synid}")
        mock_logger.info.assert_any_call(f"RELEASE_FILES_TABLE_SYNID:{mock_release_files_table_synid}")

@pytest.mark.parametrize(
    "form,clinical_link_synid,expected_cols",
    [
        ("patient_characteristics", "syn34567","ETHNICITY_DETAILED,PRIMARY_RACE_DETAILED"),
        ("cancer_panel_test", "syn23456","SEQ_YEAR"),
    ],
    ids=["get_patient_file", "get_sample_file"]
)
def test_get_main_genie_clinical_file_no_req_cols(
    mock_synapse, mock_release_version, release_files_df, column_mapping_table, form, clinical_link_synid,expected_cols
):
    expected_error = (f"Clinical file pulled from {clinical_link_synid} link is missing an expected column. \\n"
                     f"Expected columns: ['{expected_cols}']")
    with mock.patch.object(utilities, "download_synapse_table", return_value = release_files_df) as mock_download_synapse_table, pytest.raises(AssertionError) as excinfo:
        mock_syn, mock_release_files_table_synid = mock_synapse
        mock_logger = mock.MagicMock(spec=logging.Logger)
        pd.read_csv = mock.MagicMock(return_value=pd.DataFrame({"col1": [1, 2, 3]}))

        # Call the function and assert the assertion error is raised
        results = get_main_genie_clinical_file(
               mock_syn, mock_release_version, mock_release_files_table_synid, form = form, column_mapping_table =column_mapping_table, logger = mock_logger
            )
        # validate
        mock_download_synapse_table.assert_called_with(mock_syn, mock_release_files_table_synid)
        mock_syn.get.assert_called_with(clinical_link_synid, followLink=True)
        pd.read_csv.assert_called_once_with(
            "path/to/clinical_file.csv", sep="\t", skiprows=4
        )
        pd.testing.assert_frame_equal(results, pd.DataFrame({"col1": [1, 2, 3]}))
        assert str(excinfo.value) == expected_error
        mock_logger.info.assert_any_call(f"CLINICAL_FILE_LINK:{clinical_link_synid}")
        mock_logger.info.assert_any_call(f"RELEASE_FILES_TABLE_SYNID:{mock_release_files_table_synid}")

@pytest.mark.skip(reason="This test is skipped because this integration test doesn't work in pytest env")
def test_get_main_genie_clinical_sample_file_integration_test(config, column_mapping_table):
    syn = synapseclient.login()
    get_main_genie_clinical_file(
       syn,
       release=config["main_genie_release_version"], 
       release_files_table_synid=config["main_genie_data_release_files"], 
       form = 'patient_characteristics', 
       column_mapping_table =column_mapping_table, 
       )