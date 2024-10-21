import logging
from unittest.mock import MagicMock, create_autospec, patch

import numpy as np
import pandas as pd
import pytest
import synapseclient
from synapseclient import Schema, Table, client
from table_updates import utilities


@pytest.fixture(scope="session")
def syn():
    return create_autospec(synapseclient.Synapse)

def test_download_synapse_table_with_selected_columns(syn):
    select = "col1"
    df = pd.DataFrame({
        'col1': ['value1', 'value2']
    })
    schema = synapseclient.table.Schema(
        name="test_table",
        parent="syn123",
        column_names=["col1", "col2"],
        column_types=["STRING", "INTEGER"],
    )
    syn.tableQuery = MagicMock(return_value = Table(schema, df))
    result = utilities.download_synapse_table(syn, "syn123456", select)

    syn.tableQuery.assert_called_once_with("SELECT col1 from syn123456")
    pd.testing.assert_frame_equal(result, df)

def test_download_synapse_table_without_condition(syn):
    df = pd.DataFrame({
        'col1': ['value1', 'value2'],
        'col2': [1, 2]
    })
    schema = synapseclient.table.Schema(
        name="test_table",
        parent="syn123",
        column_names=["col1", "col2"],
        column_types=["STRING", "INTEGER"],
    )
    syn.tableQuery = MagicMock(return_value = Table(schema, df))
    result = utilities.download_synapse_table(syn, "syn123456", condition = "")

    syn.tableQuery.assert_called_once_with("SELECT * from syn123456")
    pd.testing.assert_frame_equal(result, df)

def test_download_synapse_table_with_condition(syn):
    condition = "col1 = 'value1'"
    df = pd.DataFrame({
        'col1': ['value1'],
        'col2': [1]
    })
    schema = synapseclient.table.Schema(
        name="test_table",
        parent="syn123",
        column_names=["col1", "col2"],
        column_types=["STRING", "INTEGER"],
    )
    syn.tableQuery = MagicMock(return_value = Table(schema, df))
    result = utilities.download_synapse_table(syn, "syn123456", condition = condition)

    syn.tableQuery.assert_called_once_with("SELECT * from syn123456 WHERE col1 = 'value1'")
    pd.testing.assert_frame_equal(result, df)

def test_download_synapse_table_with_na_values(syn):
    df = pd.DataFrame({
        'col1': ["NA", "value1", "None"],
        'col2': [1, 2, 3]
    })
    schema = synapseclient.table.Schema(
        name="test_table",
        parent="syn123",
        column_names=["col1", "col2"],
        column_types=["STRING", "INTEGER"],
    )
    syn.tableQuery = MagicMock(return_value = Table(schema, df))
    result = utilities.download_synapse_table(syn, "syn123456", condition = "")

    syn.tableQuery.assert_called_once_with("SELECT * from syn123456")
    # Unlike None is not converted to nan
    pd.testing.assert_frame_equal(result, pd.DataFrame({
        'col1': [np.nan, "value1", "None"],
        'col2': [1, 2, 3]
    }))

def test_download_synapse_table_with_empty_table(syn):
    df = pd.DataFrame(columns = ["col1", "col2"])
    schema = synapseclient.table.Schema(
        name="test_table",
        parent="syn123",
        column_names=["col1", "col2"],
        column_types=["STRING", "INTEGER"],
    )
    syn.tableQuery = MagicMock(return_value = Table(schema, df))
    result = utilities.download_synapse_table(syn, "syn123456", condition = "")

    syn.tableQuery.assert_called_once_with("SELECT * from syn123456")
    pd.testing.assert_frame_equal(result, df)

@pytest.fixture
def master_table():
    return pd.DataFrame(
        {
            "form": ["patient_characteristics", "cancer_panel_test"],
            "id": ["syn123", "syn456"],
        }
        )

@pytest.fixture
def column_mapping_table():
    return pd.DataFrame(
        {"genie_element": ["ETHNICITY_DETAILED", "PRIMARY_RACE_DETAILED", "SEQ_YEAR", "SAMPLE_TYPE_DETAILED"], 
        "prissmm_element": ["naaccr_ethnicity_code", "naaccr_race_code_primary", "cpt_seq_date", "cpt_sample_type"],
        "prissmm_form": ["patient_characteristics", "patient_characteristics", "cancer_panel_test", "cancer_panel_test"]}
        )

@pytest.mark.parametrize(
    "form,bpc_column_list,expected_error",
    [
        ("patient_characteristics", ['other_col'], f"Invalid bpc_column_list. Column names should be matching ['naaccr_ethnicity_code', 'naaccr_race_code_primary']."),
        ("cancer_panel_test", ['other_col'], f"Invalid bpc_column_list. Column names should be matching ['cpt_seq_date']."),
    ],
    ids=["invalid_patient_col", "invalid_sample_col"]
)
def test_update_tier1a_invalid_bpc_column_list(syn, form, column_mapping_table, bpc_column_list, expected_error):
    with pytest.raises(AssertionError) as excinfo:
        mock_logger = MagicMock(spec=logging.Logger)
        master_table = MagicMock()
        main_genie_table = MagicMock()

        utilities.update_tier1a(syn, form, master_table, main_genie_table, column_mapping_table, bpc_column_list, mock_logger)

        # validate
        assert str(excinfo.value) == expected_error
        mock_logger.assert_not_called()


@pytest.mark.parametrize(
    "form,cpt_table_id, main_genie_table,cpt_table_schema,cpt_dat,bpc_column_list,expected_cpt_seq_dat",
    [
        ("patient_characteristics", 
         "syn123",
         pd.DataFrame({"PATIENT_ID": ["GEN_1", "GEN_2"], "ETHNICITY_DETAILED": ["a", "b"],"PRIMARY_RACE_DETAILED": ["c", "d"], "OTHER_ID": ["test1", "test2"] }),
         synapseclient.table.Schema(name='Patient Characteristics Table',parent="syn123456",column_names=["genie_patient_id", "naaccr_ethnicity_code", "naaccr_race_code_primary", "OTHER_ID"],column_types=["STRING", "STRING", "STRING","STRING"]),
         pd.DataFrame({"genie_patient_id": ["GEN_1"], "naaccr_ethnicity_code": ["e"],"naaccr_race_code_primary": ["f"], "OTHER_ID": ["test3"]}, index=["indx"]),
         ["naaccr_ethnicity_code"],
         pd.DataFrame({"naaccr_ethnicity_code": ["a"]},index = ["indx"]),
         ),
        ("cancer_panel_test", 
         "syn456",
         pd.DataFrame({"SAMPLE_ID": ["GEN_1", "GEN_2"], "SEQ_YEAR": ["1111.0", "2222.0"],"SAMPLE_TYPE_DETAILED": ["c", "d"], "OTHER_ID": ["test1", "test2"] }),
         synapseclient.table.Schema(name='Cancer Panel Test Table',parent="syn123456",column_names=["cpt_genie_sample_id", "cpt_seq_date", "cpt_sample_type", "OTHER_ID"],column_types=["STRING", "STRING", "STRING","STRING"]),
         pd.DataFrame({"cpt_genie_sample_id": ["GEN_1", "GEN_2"], "cpt_seq_date": ["3333","4444"],"cpt_sample_type": ["e", "f"], "OTHER_ID": ["test3", "test4"] },index=["indx1","indx2"]),
         ["cpt_sample_type","cpt_seq_date"],
         pd.DataFrame({"cpt_sample_type": ["c", "d"], "cpt_seq_date": ["1111", "2222"]},index=["indx1","indx2"]),
         ),
         
    ],
    ids=["overwrite_patient_tier1_partial_col_all", "overwrite_sample_tier1_col_all"]
)
def test_update_tier1a_pass(syn, form, master_table, cpt_table_id, main_genie_table, column_mapping_table, cpt_table_schema, cpt_dat, bpc_column_list,expected_cpt_seq_dat):
    with patch.object(utilities, "download_synapse_table", return_value = cpt_dat) as mock_download_synapse_table, patch.object(syn, "tableQuery") as patch_table_query:
        logger = MagicMock(spec=logging.Logger)
        # call the function
        table_id, cpt_seq_dat = utilities.update_tier1a(syn, form, master_table, main_genie_table, column_mapping_table, bpc_column_list, logger)

        # validate
        logger.info.assert_any_call(f"Update {bpc_column_list} in {form}")
        mock_download_synapse_table.assert_called_with(syn, cpt_table_id)
        assert table_id == cpt_table_id
        pd.testing.assert_frame_equal(cpt_seq_dat, expected_cpt_seq_dat)

@pytest.mark.parametrize(
    "form,cpt_table_id, main_genie_table,cpt_table_schema,cpt_dat,bpc_column_list,expected_cpt_seq_dat, cohort",
    [
        ("patient_characteristics", 
         "syn123",
         pd.DataFrame({"PATIENT_ID": ["GEN_1", "GEN_2"], "ETHNICITY_DETAILED": ["a", "b"],"PRIMARY_RACE_DETAILED": ["c", "d"], "OTHER_ID": ["test1", "test2"] }),
         synapseclient.table.Schema(name='Patient Characteristics Table',parent="syn123456",column_names=["genie_patient_id", "naaccr_ethnicity_code", "naaccr_race_code_primary", "OTHER_ID"],column_types=["STRING", "STRING", "STRING","STRING"]),
         pd.DataFrame({"genie_patient_id": ["GEN_1", "GEN_3"], "naaccr_ethnicity_code": ["e", "f"],"naaccr_race_code_primary": ["g", "h"], "cohort": ["cohort1", "cohort2"],"OTHER_ID": ["test3", "test4"] }, index=["indx1","indx2"]),
         ["naaccr_ethnicity_code"],
         pd.DataFrame({"naaccr_ethnicity_code": ["a",np.nan]},index = ["indx1", "indx2"]),
         "cohort1"
         ),
        ("cancer_panel_test", 
         "syn456",
         pd.DataFrame({"SAMPLE_ID": ["GEN_1", "GEN_2"], "SEQ_YEAR": ["1111.0", "2222.0"],"SAMPLE_TYPE_DETAILED": ["c", "d"], "OTHER_ID": ["test1", "test2"] }),
         synapseclient.table.Schema(name='Cancer Panel Test Table',parent="syn123456",column_names=["cpt_genie_sample_id", "cpt_seq_date", "cpt_sample_type", "OTHER_ID"],column_types=["STRING", "STRING", "STRING","STRING"]),
         pd.DataFrame({"cpt_genie_sample_id": ["GEN_1", "GEN_2"], "cpt_seq_date": ["3333","4444"],"cpt_sample_type": ["e", "f"],  "cohort": ["cohort1", "cohort1"],"OTHER_ID": ["test3", "test4"] },index=["indx1","indx2"]),
         ["cpt_sample_type","cpt_seq_date"],
         pd.DataFrame({"cpt_sample_type": ["c", "d"], "cpt_seq_date": ["1111", "2222"]},index=["indx1","indx2"]),
         "cohort1"
         ),
         
    ],
    ids=["overwrite_patient_tier1_partial_col_cohort", "overwrite_sample_tier1_col_cohort"]
)
def test_update_tier1a_pass(syn, form, master_table, cpt_table_id, main_genie_table, column_mapping_table, cpt_table_schema, cpt_dat, bpc_column_list,expected_cpt_seq_dat, cohort):
    with patch.object(utilities, "download_synapse_table", return_value = cpt_dat) as mock_download_synapse_table, patch.object(syn, "tableQuery") as patch_table_query:
        logger = MagicMock(spec=logging.Logger)
        # call the function
        table_id, cpt_seq_dat = utilities.update_tier1a(syn, form, master_table, main_genie_table, column_mapping_table, bpc_column_list, logger, cohort)

        # validate
        logger.info.assert_any_call(f"Update {bpc_column_list} in {form}")
        mock_download_synapse_table.assert_called_with(syn, cpt_table_id, condition="cohort = 'cohort1'")
        assert table_id == cpt_table_id
        pd.testing.assert_frame_equal(cpt_seq_dat, expected_cpt_seq_dat)

@pytest.mark.parametrize(
    "form,cpt_table_id,cpt_table_schema,bpc_column_list,cpt_seq_dat",
    [
        ("patient_characteristics", 
         "syn123",
         synapseclient.table.Schema(name='Patient Characteristics Table',parent="syn123456",column_names=["genie_patient_id", "naaccr_ethnicity_code", "naaccr_race_code_primary", "OTHER_ID"],column_types=["STRING", "STRING", "STRING","STRING"]),
         ["naaccr_ethnicity_code"],
         pd.DataFrame({"naaccr_ethnicity_code": ["a"]},index = ["indx"]),
         ),
        ("cancer_panel_test", 
         "syn456",
         synapseclient.table.Schema(name='Cancer Panel Test Table',parent="syn123456",column_names=["cpt_genie_sample_id", "cpt_seq_date", "cpt_sample_type", "OTHER_ID"],column_types=["STRING", "STRING", "STRING","STRING"]),
         ["cpt_sample_type","cpt_seq_date"],
         pd.DataFrame({"cpt_sample_type": ["c", "d"], "cpt_seq_date": ["1111", "2222"]},index=["indx1","indx2"]),
         ),
         
    ],
    ids=["overwrite_patient_tier1_partial_col_all", "overwrite_sample_tier1_col_all"]
)
def test_overwrite_tier1a(syn, form, cpt_table_id, cpt_table_schema, bpc_column_list,cpt_seq_dat):
    with patch.object(syn, "tableQuery") as patch_table_query, patch.object(syn, "store") as patch_store:
        logger = MagicMock(spec=logging.Logger)
        syn.get = MagicMock(return_value = cpt_table_schema)
        patch_table_query.return_value =  MagicMock(etag = "test_etag")
        
        # call the function
        utilities.overwrite_tier1a(syn, form, cpt_table_id, cpt_seq_dat, bpc_column_list, logger)

        # validate
        logger.info.assert_any_call(f"Overwrite {bpc_column_list} in {form}")
        syn.get.assert_called_with(cpt_table_id)
        syn.tableQuery.assert_called_with(f"SELECT * FROM {cpt_table_id}")
        args, kwargs = patch_store.call_args
        stored_table = args[0]
        assert stored_table.schema == cpt_table_schema
        pd.testing.assert_frame_equal(stored_table.asDataFrame(), cpt_seq_dat.reset_index(drop=True))
        assert stored_table.etag == "test_etag"