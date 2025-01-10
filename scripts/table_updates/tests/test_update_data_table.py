from unittest.mock import MagicMock, create_autospec, patch

import numpy as np
import pandas as pd
import pytest
import synapseclient
import update_data_table
import utilities
from synapseclient import Schema, Table, client
from update_data_table import *


@pytest.fixture(scope="session")
def syn():
    return create_autospec(synapseclient.Synapse)

@pytest.fixture
def master_table():
    return pd.DataFrame(
        {
            "form": ["patient_characteristics", "cancer_panel_test"],
            "id": ["syn123", "syn456"],
        }
        )

@pytest.fixture
def mock_synapse(syn):
    # returns the mocked syn and release table synid
    syn = MagicMock()
    release_files_table_synid = "syn12345"
    # Mock the Link entity retrieval
    clinical_link_ent_mock = {
        "linksTo": {"targetId": "syn88888", "targetVersionNumber": 3}
    }
    syn.get.return_value = MagicMock(path="path/to/clinical_file.csv")
    return syn, release_files_table_synid

@pytest.fixture
def mock_release_version():
    return "v1.0"

@pytest.fixture
def column_mapping_table():
    return pd.DataFrame(
        {"genie_element": ["ETHNICITY_DETAILED", "PRIMARY_RACE_DETAILED", "SEQ_YEAR", "SAMPLE_TYPE_DETAILED"], 
        "prissmm_element": ["naaccr_ethnicity_code", "naaccr_race_code_primary", "cpt_seq_date", "cpt_sample_type"],
        "prissmm_form": ["patient_characteristics", "patient_characteristics", "cancer_panel_test", "cancer_panel_test"]}
        )

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
        "tier1a_replacement_mapping":{
            "patient_characteristics_tier1a_replacement_mapping_table": "syn22222",
            "cancer_panel_test_tier1a_replacement_mapping_table": "syn33333"
        },
        "main_genie_release_version": "16.6-consortium",
        "main_genie_data_release_files": "syn16804261",
        "main_genie_sample_mapping_table": "syn7434273",
        "patient_tier1a_column_list_to_be_replaced": [
            "naaccr_ethnicity_code",
            "naaccr_race_code_primary",
            "naaccr_race_code_secondary",
            "naaccr_race_code_tertiary",
            "naaccr_sex_code"
        ],
    "sample_tier1a_column_list_to_be_replaced": [
        "cpt_sample_type", "cpt_seq_date"
        ]
    }

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
        (pd.DataFrame({"SAMPLE_ID": [1, 2, 3], "SEQ_YEAR": [2014, 2014, 2013], "SAMPLE_TYPE_DETAILED": [1, 2, 3],"OTHER_ID": [6, 7, 8]}), "cancer_panel_test", "syn23456"),
    ],
    ids=["get_patient_file", "get_sample_file"]
)
def test_get_main_genie_clinical_file_success(
    mock_synapse, mock_release_version, release_files_df, column_mapping_table, clinical_df_mock, form, clinical_link_synid
):
    with patch.object(utilities, "download_synapse_table", return_value = release_files_df) as mock_download_synapse_table, patch.object(pandas,"read_csv", return_value=clinical_df_mock) as mock_read_csv:
 
        syn, mock_release_files_table_synid = mock_synapse
        mock_logger = MagicMock(spec=logging.Logger)
  
        # Call the function
        results = get_main_genie_clinical_file(
               syn, mock_release_version, mock_release_files_table_synid, form = form, column_mapping_table =column_mapping_table, logger = mock_logger
            )
        # validate
        mock_download_synapse_table.assert_called_with(syn, mock_release_files_table_synid)
        syn.get.assert_called_with(clinical_link_synid, followLink=True)
        mock_read_csv.assert_called_once_with(
            "path/to/clinical_file.csv", sep="\t", skiprows=4
        )
        pd.testing.assert_frame_equal(results, clinical_df_mock)
        mock_logger.info.assert_any_call(f"CLINICAL_FILE_LINK:{clinical_link_synid}")
        mock_logger.info.assert_any_call(f"RELEASE_FILES_TABLE_SYNID:{mock_release_files_table_synid}")


@pytest.mark.parametrize(
    "clinical_df_mock, form,clinical_link_synid",
    [
        (pd.DataFrame(), "patient_characteristics", "syn34567"),
        (pd.DataFrame(), "cancer_panel_test", "syn23456"),
    ],
    ids=["get_patient_file_empty", "get_sample_file_empty"]
)
def test_get_main_genie_clinical_file_empty_file(
    mock_synapse, mock_release_version, release_files_df, column_mapping_table, form, clinical_df_mock, clinical_link_synid
):
    with patch.object(utilities, "download_synapse_table", return_value = release_files_df) as mock_download_synapse_table, patch.object(pandas,"read_csv", return_value=clinical_df_mock) as mock_read_csv, pytest.raises(ValueError, match=f"Clinical file pulled from {clinical_link_synid} link is empty."):
        # Mock pandas.read_csv to return an empty DataFrame
        syn, mock_release_files_table_synid = mock_synapse
        mock_logger = MagicMock(spec=logging.Logger)

        # Call the function and assert the assertion error is raised
        results = get_main_genie_clinical_file(
               syn, mock_release_version, mock_release_files_table_synid, form = form, column_mapping_table =column_mapping_table, logger = mock_logger
            )

        # validate
        mock_download_synapse_table.assert_called_with(syn, mock_release_files_table_synid)
        syn.get.assert_called_with(clinical_link_synid, followLink=True)
        mock_read_csv.assert_called_once_with(
            "path/to/clinical_file.csv", sep="\t", skiprows=4
        )
        pd.testing.assert_frame_equal(results, clinical_df_mock)
        mock_logger.info.assert_any_call(f"CLINICAL_FILE_LINK:{clinical_link_synid}")
        mock_logger.info.assert_any_call(f"RELEASE_FILES_TABLE_SYNID:{mock_release_files_table_synid}")

@pytest.mark.parametrize(
    "clinical_df_mock,form,clinical_link_synid,expected_cols",
    [
        (pd.DataFrame({"col1": [1, 2, 3]}), "patient_characteristics", "syn34567","ETHNICITY_DETAILED,PRIMARY_RACE_DETAILED"),
        (pd.DataFrame({"col1": [1, 2, 3]}), "cancer_panel_test", "syn23456","SEQ_YEAR"),
    ],
    ids=["get_patient_file", "get_sample_file"]
)
def test_get_main_genie_clinical_file_no_req_cols(
    mock_synapse, mock_release_version, release_files_df, column_mapping_table, form, clinical_df_mock, clinical_link_synid, expected_cols
):
    expected_error = (f"Clinical file pulled from {clinical_link_synid} link is missing an expected column. \\n"
                     f"Expected columns: ['{expected_cols}']")
    with patch.object(utilities, "download_synapse_table", return_value = release_files_df) as mock_download_synapse_table,patch.object(pandas,"read_csv", return_value=clinical_df_mock) as mock_read_csv,  pytest.raises(ValueError) as excinfo:
        syn, mock_release_files_table_synid = mock_synapse
        mock_logger = MagicMock(spec=logging.Logger)

        # Call the function and assert the assertion error is raised
        results = get_main_genie_clinical_file(
               syn, mock_release_version, mock_release_files_table_synid, form = form, column_mapping_table =column_mapping_table, logger = mock_logger
            )
        # validate
        mock_download_synapse_table.assert_called_with(syn, mock_release_files_table_synid)
        syn.get.assert_called_with(clinical_link_synid, followLink=True)
        mock_read_csv.assert_called_once_with(
            "path/to/clinical_file.csv", sep="\t", skiprows=4
        )
        pd.testing.assert_frame_equal(results, clinical_df_mock)
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
    
@pytest.mark.parametrize(
    "form,bpc_column_list,expected_error",
    [
        ("patient_characteristics", ['other_col'], f"Invalid bpc_column_list. Column names should be matching ['naaccr_ethnicity_code', 'naaccr_race_code_primary']."),
        ("cancer_panel_test", ['other_col'], f"Invalid bpc_column_list. Column names should be matching ['cpt_seq_date']."),
    ],
    ids=["invalid_patient_col", "invalid_sample_col"]
)
def test_update_tier1a_invalid_bpc_column_list(syn, form, column_mapping_table, bpc_column_list, expected_error, config):
    with pytest.raises(ValueError) as excinfo:
        mock_logger = MagicMock(spec=logging.Logger)
        master_table = MagicMock()
        main_genie_table = MagicMock()

        update_tier1a(syn, form, master_table, main_genie_table, column_mapping_table, bpc_column_list, config, mock_logger)   

        # validate
        assert str(excinfo.value) == expected_error
        mock_logger.assert_not_called()

def get__update_tier1a_pass_all_cohorts_test_cases():
    return [
        {
        "name": "update_patient_tier1_partial_col_all_cohorts",
        "form": "patient_characteristics", 
        "cpt_table_id": "syn123",
        "main_genie_table": pd.DataFrame({"PATIENT_ID": ["GEN_1", "GEN_2"], "ETHNICITY_DETAILED": ["a", "b"],"PRIMARY_RACE_DETAILED": ["c", "d"], "OTHER_ID": ["test1", "test2"] }),
        "cpt_table_schema": synapseclient.table.Schema(name='Patient Characteristics Table',parent="syn123456",column_names=["genie_patient_id", "naaccr_ethnicity_code", "naaccr_race_code_primary", "OTHER_ID"],column_types=["STRING", "STRING", "STRING","STRING"]),
        "cpt_dat": pd.DataFrame({"genie_patient_id": ["GEN_1"], "naaccr_ethnicity_code": ["e"],"naaccr_race_code_primary": ["f"], "OTHER_ID": ["test3"]}, index=["indx"]),
        "bpc_column_list": ["naaccr_ethnicity_code"],
        "expected_cpt_seq_dat": pd.DataFrame({"naaccr_ethnicity_code": ["a"]},index = ["indx"]),
        },
        {
        "name": "update_sample_tier1_col_all_cohorts",
        "form": "cancer_panel_test", 
        "cpt_table_id": "syn456",
        "main_genie_table": pd.DataFrame({"SAMPLE_ID": ["GEN_1", "GEN_2"], "SEQ_YEAR": ["1111.0", "2222.0"],"SAMPLE_TYPE_DETAILED": ["c", "d"], "OTHER_ID": ["test1", "test2"] }),
        "cpt_table_schema": synapseclient.table.Schema(name='Cancer Panel Test Table',parent="syn123456",column_names=["cpt_genie_sample_id", "cpt_seq_date", "cpt_sample_type", "OTHER_ID"],column_types=["STRING", "STRING", "STRING","STRING"]),
        "cpt_dat": pd.DataFrame({"cpt_genie_sample_id": ["GEN_1", "GEN_2"], "cpt_seq_date": ["3333","4444"],"cpt_sample_type": ["e", "f"], "OTHER_ID": ["test3", "test4"] },index=["indx1","indx2"]),
        "bpc_column_list": ["cpt_sample_type","cpt_seq_date"],
        "expected_cpt_seq_dat": pd.DataFrame({"cpt_sample_type": ["c", "d"], "cpt_seq_date": ["1111", "2222"]},index=["indx1","indx2"]),
        }

    ]

@pytest.mark.parametrize(
    "test_cases", get__update_tier1a_pass_all_cohorts_test_cases(), ids=lambda x: x["name"]
)
def test_update_tier1a_pass_all_cohorts(syn, test_cases, master_table, column_mapping_table, config):
    with patch.object(utilities, "download_synapse_table", return_value = test_cases["cpt_dat"]) as mock_download_synapse_table, patch.object(syn, "tableQuery") as patch_table_query, patch.object(utilities, "update_tier1a_data_replacement_mapping_table", return_value = None) as mock_update_tier1a:
        logger = MagicMock(spec=logging.Logger)

        # call the function
        table_id, cpt_seq_dat = update_tier1a(syn, test_cases["form"], master_table, test_cases["main_genie_table"], column_mapping_table, test_cases["bpc_column_list"], config, logger)
        #import pdb; pdb.set_trace()
        # validate
        logger.info.assert_called_with(f"Update {test_cases['bpc_column_list']} in {test_cases['form']}")
        mock_update_tier1a.assert_called_once()
        mock_download_synapse_table.assert_called_with(syn, test_cases["cpt_table_id"])
        assert table_id == test_cases["cpt_table_id"]
        pd.testing.assert_frame_equal(cpt_seq_dat, test_cases["expected_cpt_seq_dat"])

def get__update_tier1a_pass_cohort_specific_test_cases():
    return [
        {
        "name": "update_patient_tier1_partial_col_cohort",
        "form": "patient_characteristics",
        "cpt_table_id": "syn123",
        "main_genie_table": pd.DataFrame({"PATIENT_ID": ["GEN_1", "GEN_3"], "ETHNICITY_DETAILED": ["a", "b"],"PRIMARY_RACE_DETAILED": ["c", "d"], "OTHER_ID": ["test1", "test2"] }),
        "cpt_table_schema": synapseclient.table.Schema(name='Patient Characteristics Table',parent="syn123456",column_names=["genie_patient_id", "naaccr_ethnicity_code", "naaccr_race_code_primary", "OTHER_ID"],column_types=["STRING", "STRING", "STRING","STRING"]),
        "cpt_dat": pd.DataFrame({"genie_patient_id": ["GEN_1", "GEN_2"], "naaccr_ethnicity_code": ["e", "f"],"naaccr_race_code_primary": ["g", "h"], "cohort": ["cohort1", "cohort1"],"OTHER_ID": ["test3", "test4"] }, index=["indx1", "indx2"]),
        "bpc_column_list": ["naaccr_ethnicity_code"],
        "expected_cpt_seq_dat": pd.DataFrame({"naaccr_ethnicity_code": ["a",np.nan]},index = ["indx1", "indx2"]),
        "cohort": "cohort1"
        },
        {
        "name": "update_sample_tier1_col_cohort",
        "form": "cancer_panel_test", 
        "cpt_table_id": "syn456",
        "main_genie_table": pd.DataFrame({"SAMPLE_ID": ["GEN_1", "GEN_2"], "SEQ_YEAR": ["1111.0", "2222.0"],"SAMPLE_TYPE_DETAILED": ["c", "d"], "OTHER_ID": ["test1", "test2"] }),
        "cpt_table_schema": synapseclient.table.Schema(name='Cancer Panel Test Table',parent="syn123456",column_names=["cpt_genie_sample_id", "cpt_seq_date", "cpt_sample_type", "OTHER_ID"],column_types=["STRING", "STRING", "STRING","STRING"]),
        "cpt_dat": pd.DataFrame({"cpt_genie_sample_id": ["GEN_1", "GEN_2"], "cpt_seq_date": ["3333","4444"],"cpt_sample_type": ["e", "f"],  "cohort": ["cohort1", "cohort1"],"OTHER_ID": ["test3", "test4"] },index=["indx1","indx2"]),
        "bpc_column_list": ["cpt_sample_type","cpt_seq_date"],
        "expected_cpt_seq_dat": pd.DataFrame({"cpt_sample_type": ["c", "d"], "cpt_seq_date": ["1111", "2222"]},index=["indx1","indx2"]),
        "cohort": "cohort1"
        }
    ]

@pytest.mark.parametrize(
    "test_cases", get__update_tier1a_pass_cohort_specific_test_cases(), ids=lambda x: x["name"]
)
def test_update_tier1a_pass_cohort_specific(syn, test_cases, master_table, column_mapping_table, config):
    with patch.object(utilities, "download_synapse_table", return_value = test_cases["cpt_dat"]) as mock_download_synapse_table, patch.object(syn, "tableQuery") as patch_table_query, patch.object(utilities, "update_tier1a_data_replacement_mapping_table", return_value = None) as mock_update_tier1a:
        logger = MagicMock(spec=logging.Logger)
        # call the function
        table_id, cpt_seq_dat = update_tier1a(syn, test_cases["form"], master_table, test_cases["main_genie_table"], column_mapping_table, test_cases["bpc_column_list"], config, logger, test_cases["cohort"])

        # validate
        logger.info.assert_called_with(f"Update {test_cases['bpc_column_list']} in {test_cases['form']}")
        if test_cases["form"] == "patient_characteristics":
            logger.warning.assert_called_with("Missing GEN_2 in main GENIE table. Please be advised that tier 1a fields for these records will be filled with NaN.")
        mock_update_tier1a.assert_called_once()
        mock_download_synapse_table.assert_called_with(syn, test_cases["cpt_table_id"], condition="cohort = 'cohort1'")
        assert table_id == test_cases["cpt_table_id"]
        pd.testing.assert_frame_equal(cpt_seq_dat, test_cases["expected_cpt_seq_dat"])

def get__overwrite_tier1a_test_cases():
    return [
        {
        "name": "overwrite_patient_tier1",
        "form": "patient_characteristics",
        "cpt_table_id": "syn123",
        "cpt_table_schema": synapseclient.table.Schema(name='Patient Characteristics Table',parent="syn123456",column_names=["genie_patient_id", "naaccr_ethnicity_code", "naaccr_race_code_primary", "OTHER_ID"],column_types=["STRING", "STRING", "STRING","STRING"]),
        "bpc_column_list": ["naaccr_ethnicity_code"],
        "cpt_seq_dat": pd.DataFrame({"naaccr_ethnicity_code": ["a"]},index = ["indx"]),
        },
        {
        "name": "overwrite_sample_tier1",
        "form": "cancer_panel_test",
        "cpt_table_id": "syn456",
        "cpt_table_schema": synapseclient.table.Schema(name='Cancer Panel Test Table',parent="syn123456",column_names=["cpt_genie_sample_id", "cpt_seq_date", "cpt_sample_type", "OTHER_ID"],column_types=["STRING", "STRING", "STRING","STRING"]),
        "bpc_column_list": ["cpt_sample_type","cpt_seq_date"],
        "cpt_seq_dat": pd.DataFrame({"cpt_sample_type": ["c", "d"], "cpt_seq_date": ["1111", "2222"]},index=["indx1","indx2"]),
        }
    ]

@pytest.mark.parametrize(
    "test_cases", get__overwrite_tier1a_test_cases(), ids=lambda x: x["name"]
)
def test_overwrite_tier1a(syn, test_cases):
    with patch.object(syn, "tableQuery") as patch_table_query, patch.object(syn, "store") as patch_store:
        logger = MagicMock(spec=logging.Logger)
        syn.get = MagicMock(return_value = test_cases["cpt_table_schema"])
        patch_table_query.return_value =  MagicMock(etag = "test_etag")
        
        # call the function
        overwrite_tier1a(syn, test_cases["form"], test_cases["cpt_table_id"], test_cases["cpt_seq_dat"], test_cases["bpc_column_list"], logger)

        # validate
        logger.info.assert_called_with(f"Overwrite {test_cases['bpc_column_list']} in {test_cases['form']}")
        syn.get.assert_called_with(test_cases["cpt_table_id"])
        syn.tableQuery.assert_called_with(f"SELECT * FROM {test_cases['cpt_table_id']}")
        args, kwargs = patch_store.call_args
        stored_table = args[0]
        assert stored_table.schema == test_cases["cpt_table_schema"]
        pd.testing.assert_frame_equal(stored_table.asDataFrame(), test_cases["cpt_seq_dat"].reset_index(drop=True))
        assert stored_table.etag == "test_etag"


@pytest.mark.parametrize(
    "left_table,right_table,left_on,right_on",
    [
        (
        pd.DataFrame({'ID': [1, 2, 3], 'Value1': ['A', 'B', 'C']}),
        pd.DataFrame({'ID': [1, 4, 5], 'Value2': ['X', 'Y', 'Z']}),
        "ID",
        "ID"
        ),
        (
        pd.DataFrame({'ID_1': [1, 2, 3], 'Value1': ['A', 'B', 'C']}),
        pd.DataFrame({'ID_2': [1, 4, 5], 'Value2': ['X', 'Y', 'Z']}),
        "ID_1",
        "ID_2"
        ),

    ],
    ids=["same_join_key_name", "diff_join_key_name"]

)
def test_check_if_all_join_keys_available_fail(left_table, right_table, left_on, right_on):
    logger = MagicMock(spec=logging.Logger)
    logger.warning = MagicMock()
    # call the function
    check_if_all_join_keys_available(left_table, right_table, left_on, right_on, logger)

    #  validate
    logger.warning.assert_called_with("Missing 2, 3 in main GENIE table. Please be advised that tier 1a fields for these records will be filled with NaN.")

@pytest.mark.parametrize(
    "left_table,right_table,left_on,right_on",
    [
        (
        pd.DataFrame({'ID': [1, 2, 3], 'Value1': ['A', 'B', 'C']}),
        pd.DataFrame({'ID': [1, 2, 3], 'Value2': ['X', 'Y', 'Z']}),
        "ID",
        "ID"
        ),
        (
        pd.DataFrame({'ID_1': [1, 2, 3], 'Value1': ['A', 'B', 'C']}),
        pd.DataFrame({'ID_2': [1, 2, 3], 'Value2': ['X', 'Y', 'Z']}),
        "ID_1",
        "ID_2"
        )
    ],
    ids=["same_join_key_name", "diff_join_key_name"]
)
def test_check_if_all_join_keys_available_pass(left_table, right_table, left_on, right_on):
    logger = MagicMock(spec=logging.Logger)
    logger.warning = MagicMock()
    check_if_all_join_keys_available(left_table, right_table, left_on, right_on, logger)

    #  validate
    logger.warning.assert_not_called()