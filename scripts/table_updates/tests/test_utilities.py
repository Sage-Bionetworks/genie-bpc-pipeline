from unittest.mock import MagicMock, create_autospec, patch

import numpy as np
import pandas as pd
import pytest
import synapseclient
from synapseclient import Schema, Table, client
from utilities import *


@pytest.fixture(scope="session")
def syn():
    return create_autospec(synapseclient.Synapse)


@pytest.fixture(scope="session")
def table_schema():
    schema = synapseclient.table.Schema(
        id="syn123456",
        name="test_table",
        parent="syn123",
        column_names=["col1", "col2"],
        column_types=["STRING", "INTEGER"],
    )

    return schema


@pytest.fixture
def config():
    yield {
        "tier1a_replacement_mapping": {
            "patient_characteristics_tier1a_replacement_mapping_table": "syn22222",
            "cancer_panel_test_tier1a_replacement_mapping_table": "syn33333",
        },
        "main_genie_release_version": "V1",
    }


@pytest.mark.parametrize(
    "query_return_df,select,query,expected_df",
    [
        (
            pd.DataFrame({"col1": ["value1", "value2"]}),
            "col1",
            "SELECT col1 from syn123456",
            pd.DataFrame({"col1": ["value1", "value2"]}),
        ),
        (
            pd.DataFrame({"col1": ["value1", "value2"], "col2": [1, 2]}),
            "col1,col2",
            "SELECT col1,col2 from syn123456",
            pd.DataFrame({"col1": ["value1", "value2"], "col2": [1, 2]}),
        ),
        (
            pd.DataFrame({"col1": ["NA", "value1", "None"], "col2": [1, 2, 3]}),
            "*",
            "SELECT * from syn123456",
            pd.DataFrame({"col1": [np.nan, "value1", "None"], "col2": [1, 2, 3]}),
        ),
        (
            pd.DataFrame(columns=["col1", "col2"]),
            "*",
            "SELECT * from syn123456",
            pd.DataFrame(columns=["col1", "col2"]),
        ),
    ],
    ids=[
        "selected_single_column",
        "selected_multiple_column",
        "pull_table_with_na_values_all_columns",
        "pull_empty_table_all_columns",
    ],
)
def test_download_synapse_table_default_condition(
    syn, table_schema, query_return_df, select, query, expected_df
):
    syn.tableQuery = MagicMock(return_value=Table(table_schema, query_return_df))
    result = download_synapse_table(syn, "syn123456", select)

    # validate
    syn.tableQuery.assert_called_once_with(query)
    pd.testing.assert_frame_equal(result, expected_df)


@pytest.mark.parametrize(
    "query_return_df,condition,query,expected_df",
    [
        (
            pd.DataFrame({"col1": ["value1"], "col2": [1]}),
            "col1 = 'value1'",
            "SELECT * from syn123456 WHERE col1 = 'value1'",
            pd.DataFrame({"col1": ["value1"], "col2": [1]}),
        ),
        (
            pd.DataFrame({"col1": ["NA", "value1", "None"], "col2": [1, 1, 1]}),
            "col2 = 1",
            "SELECT * from syn123456 WHERE col2 = 1",
            pd.DataFrame({"col1": [np.nan, "value1", "None"], "col2": [1, 1, 1]}),
        ),
    ],
    ids=["selected_row_all_columns", "pull_table_with_na_values_all_columns"],
)
def test_download_synapse_table_with_condition(
    syn, table_schema, query_return_df, condition, query, expected_df
):
    syn.tableQuery = MagicMock(return_value=Table(table_schema, query_return_df))
    result = download_synapse_table(syn, "syn123456", condition=condition)

    # validate
    syn.tableQuery.assert_called_once_with(query)
    pd.testing.assert_frame_equal(result, expected_df)


@pytest.mark.parametrize(
    "query_return_df,select,condition,query,expected_df",
    [
        (
            pd.DataFrame({"col1": ["value1"], "col2": [1]}),
            "col1",
            "col1 = 'value1'",
            "SELECT col1 from syn123456 WHERE col1 = 'value1'",
            pd.DataFrame({"col1": ["value1"], "col2": [1]}),
        ),
        (
            pd.DataFrame({"col1": ["value1"], "col2": [1]}),
            "col1,col2",
            "col1 = 'value1'",
            "SELECT col1,col2 from syn123456 WHERE col1 = 'value1'",
            pd.DataFrame({"col1": ["value1"], "col2": [1]}),
        ),
    ],
    ids=[
        "selected_one_columns_with_condition",
        "select_multiple_columns_with_condition",
    ],
)
def test_download_synapse_table_with_select_and_condition(
    syn, table_schema, query_return_df, select, condition, query, expected_df
):
    syn.tableQuery = MagicMock(return_value=Table(table_schema, query_return_df))
    result = download_synapse_table(
        syn, "syn123456", select=select, condition=condition
    )

    # validate
    syn.tableQuery.assert_called_once_with(query)
    pd.testing.assert_frame_equal(result, expected_df)


def test_download_empty_synapse_table_with_condition(
    syn,
    table_schema,
):
    syn.tableQuery = MagicMock(
        return_value=Table(table_schema, pd.DataFrame(columns=["col1", "col2"]))
    )
    result = download_synapse_table(syn, "syn123456", condition="col2 = 1")

    # validate
    syn.tableQuery.assert_called_once_with("SELECT * from syn123456 WHERE col2 = 1")
    pd.testing.assert_frame_equal(result, pd.DataFrame(columns=["col1", "col2"]))


@pytest.mark.parametrize(
    "input_df,cols,expected_df",
    [
        (
            pd.DataFrame({"col1": ["\\abc", "def"], "col2": ["abc", "def\\"]}),
            ["col1", "col2"],
            pd.DataFrame({"col1": ["abc", "def"], "col2": ["abc", "def"]}),
        ),
        (
            pd.DataFrame({"col1": ["abc", "def"], "col2": ["abc", "def\\"]}),
            ["col2"],
            pd.DataFrame({"col1": ["abc", "def"], "col2": ["abc", "def"]}),
        ),
        (
            pd.DataFrame({"col1": ["abc", "def"], "col2": ["abc", "def\\"]}),
            ["col1"],
            pd.DataFrame({"col1": ["abc", "def"], "col2": ["abc", "def\\"]}),
        ),
        (
            pd.DataFrame({"col1": ["abc", "def"], "col2": ["abc", "def"]}),
            ["col1", "col2"],
            pd.DataFrame({"col1": ["abc", "def"], "col2": ["abc", "def"]}),
        ),
        (
            pd.DataFrame(
                {
                    "col1": ["\\abc", "de\\f", "ghi\\"],
                    "col2": ["abc(\\hh)", "def,\\,hh", "ghi, ,\\hh"],
                }
            ),
            ["col1", "col2"],
            pd.DataFrame(
                {
                    "col1": ["abc", "def", "ghi"],
                    "col2": ["abc(hh)", "def,,hh", "ghi, ,hh"],
                }
            ),
        ),
        (
            pd.DataFrame(
                {
                    "col1": [1, "de\\f", "ghi\\", np.nan],
                    "col2": ["abc(\\hh)", "def,\\,hh", "ghi, ,\\hh", 2],
                }
            ),
            ["col1", "col2"],
            pd.DataFrame(
                {
                    "col1": [1, "def", "ghi", np.nan],
                    "col2": ["abc(hh)", "def,,hh", "ghi, ,hh", 2],
                }
            ),
        ),
    ],
    ids=[
        "multiple_columns_with_backslash",
        "one_column_with_backslash",
        "one_column_with_backslash_but_not_selected",
        "none_column_with_backslash",
        "backslashes_in_multiple_places",
        "various_column_types",
    ],
)
def test_remove_backslash(input_df, cols, expected_df):
    results = remove_backslash(input_df, cols)
    pd.testing.assert_frame_equal(results, expected_df)


@pytest.mark.parametrize(
    "input_df,cols",
    [
        (pd.DataFrame({"col1": ["\\abc", "def"], "col2": ["abc", "def\\"]}), ["col3"]),
    ],
)
def test_remove_backslsh_fail(input_df, cols):
    with pytest.raises(
        ValueError, match="Invalid column list. Not all columns are in the dataframe."
    ):
        remove_backslash(input_df, cols)


def get__update_tier1a_data_replacement_mapping_table_test_cases():
    return [
        {
            "name": "patient_characteristics_mapping",
            "merged_table": pd.DataFrame(
                {
                    "cohort": ["A", "A", "B"],
                    "genie_patient_id": ["A-1-2", "A-2-3", "B-1-2"],
                    "site": ["1", "2", "1"],
                    "naaccr_ethnicity_code": ["A", "B", "C"],
                    "naaccr_race_code_primary": ["X", "Y", "Z"],
                    "naaccr_race_code_secondary": ["P", "Q", "R"],
                    "naaccr_race_code_tertiary": ["M", "N", "O"],
                    "naaccr_sex_code": ["M", "F", "M"],
                    "ETHNICITY_DETAILED": ["Ethnicity A", "Ethnicity B", "Ethnicity C"],
                    "PRIMARY_RACE_DETAILED": ["Race X", "Race Y", "Race Z"],
                    "SECONDARY_RACE_DETAILED": ["Race P", "Race Q", "Race R"],
                    "TERTIARY_RACE_DETAILED": ["Race M", "Race N", "Race O"],
                    "SEX_DETAILED": ["Male", "Female", "Male"],
                    "other_cols": [1, 2, 3],
                }
            ),
            "form": "patient_characteristics",
            "bpc_column_list": [
                "naaccr_ethnicity_code",
                "naaccr_race_code_primary",
                "naaccr_race_code_secondary",
                "naaccr_race_code_tertiary",
                "naaccr_sex_code",
            ],
            "main_genie_column_list": [
                "ETHNICITY_DETAILED",
                "PRIMARY_RACE_DETAILED",
                "SECONDARY_RACE_DETAILED",
                "TERTIARY_RACE_DETAILED",
                "SEX_DETAILED",
            ],
            "cohort": "A",
            "expected_df": pd.DataFrame(
                {
                    "cohort": ["A", "A", "A", "A", "A", "A", "A", "A", "A", "A"],
                    "genie_patient_id": [
                        "A-1-2",
                        "A-2-3",
                        "A-1-2",
                        "A-2-3",
                        "A-1-2",
                        "A-2-3",
                        "A-1-2",
                        "A-2-3",
                        "A-1-2",
                        "A-2-3",
                    ],
                    "site": ["1", "2", "1", "2", "1", "2", "1", "2", "1", "2"],
                    "bpc_field": [
                        "naaccr_ethnicity_code",
                        "naaccr_ethnicity_code",
                        "naaccr_race_code_primary",
                        "naaccr_race_code_primary",
                        "naaccr_race_code_secondary",
                        "naaccr_race_code_secondary",
                        "naaccr_race_code_tertiary",
                        "naaccr_race_code_tertiary",
                        "naaccr_sex_code",
                        "naaccr_sex_code",
                    ],
                    "bpc_field_value": [
                        "A",
                        "B",
                        "X",
                        "Y",
                        "P",
                        "Q",
                        "M",
                        "N",
                        "M",
                        "F",
                    ],
                    "main_genie_field": [
                        "ETHNICITY_DETAILED",
                        "ETHNICITY_DETAILED",
                        "PRIMARY_RACE_DETAILED",
                        "PRIMARY_RACE_DETAILED",
                        "SECONDARY_RACE_DETAILED",
                        "SECONDARY_RACE_DETAILED",
                        "TERTIARY_RACE_DETAILED",
                        "TERTIARY_RACE_DETAILED",
                        "SEX_DETAILED",
                        "SEX_DETAILED",
                    ],
                    "main_genie_field_value": [
                        "Ethnicity A",
                        "Ethnicity B",
                        "Race X",
                        "Race Y",
                        "Race P",
                        "Race Q",
                        "Race M",
                        "Race N",
                        "Male",
                        "Female",
                    ],
                    "Main_Genie_Release_Version": [
                        "V1",
                        "V1",
                        "V1",
                        "V1",
                        "V1",
                        "V1",
                        "V1",
                        "V1",
                        "V1",
                        "V1",
                    ],
                }
            ),
        },
        {
            "name": "cancer_panel_test_mapping",
            "merged_table": pd.DataFrame(
                {
                    "cohort": ["A", "A"],
                    "cpt_genie_sample_id": ["A-1-2", "A-2-3"],
                    "site": ["1", "2"],
                    "cpt_sample_type": ["A", "B"],
                    "cpt_seq_date": ["X", "Y"],
                    "SAMPLE_TYPE_DETAILED": ["P", "Q"],
                    "SAMPLE_TYPE": ["P1", "Q1"],
                    "SEQ_YEAR": ["M", "N"],
                    "other_cols": [1, 2],
                }
            ),
            "form": "cancer_panel_test",
            "bpc_column_list": ["cpt_sample_type", "cpt_seq_date"],
            "main_genie_column_list": ["SAMPLE_TYPE_DETAILED", "SEQ_YEAR"],
            "cohort": "A",
            "expected_df": pd.DataFrame(
                {
                    "cohort": ["A", "A", "A", "A"],
                    "cpt_genie_sample_id": ["A-1-2", "A-2-3", "A-1-2", "A-2-3"],
                    "site": ["1", "2", "1", "2"],
                    "bpc_field": [
                        "cpt_sample_type",
                        "cpt_sample_type",
                        "cpt_seq_date",
                        "cpt_seq_date",
                    ],
                    "bpc_field_value": ["A", "B", "X", "Y"],
                    "main_genie_field": [
                        "SAMPLE_TYPE_DETAILED",
                        "SAMPLE_TYPE_DETAILED",
                        "SEQ_YEAR",
                        "SEQ_YEAR",
                    ],
                    "main_genie_field_value": ["P", "Q", "M", "N"],
                    "Main_Genie_Release_Version": ["V1", "V1", "V1", "V1"],
                }
            ),
        },
        {
            "name": "cancer_panel_test_mapping_all_cohorts",
            "merged_table": pd.DataFrame(
                {
                    "cohort": ["A", "A", "B"],
                    "cpt_genie_sample_id": ["A-1-2", "A-2-3", "B-1-2"],
                    "site": ["1", "2", "1"],
                    "cpt_sample_type": ["A", "B", "C"],
                    "cpt_seq_date": ["X", "Y", "Z"],
                    "SAMPLE_TYPE_DETAILED": ["P", "Q", "R"],
                    "SEQ_YEAR": ["M", "N", "O"],
                    "other_cols": [1, 2, 3],
                }
            ),
            "form": "cancer_panel_test",
            "bpc_column_list": ["cpt_sample_type", "cpt_seq_date"],
            "main_genie_column_list": ["SAMPLE_TYPE_DETAILED", "SEQ_YEAR"],
            "cohort": "",
            "expected_df": pd.DataFrame(
                {
                    "cohort": ["A", "A", "B", "A", "A", "B"],
                    "cpt_genie_sample_id": [
                        "A-1-2",
                        "A-2-3",
                        "B-1-2",
                        "A-1-2",
                        "A-2-3",
                        "B-1-2",
                    ],
                    "site": ["1", "2", "1", "1", "2", "1"],
                    "bpc_field": [
                        "cpt_sample_type",
                        "cpt_sample_type",
                        "cpt_sample_type",
                        "cpt_seq_date",
                        "cpt_seq_date",
                        "cpt_seq_date",
                    ],
                    "bpc_field_value": ["A", "B", "C", "X", "Y", "Z"],
                    "main_genie_field": [
                        "SAMPLE_TYPE_DETAILED",
                        "SAMPLE_TYPE_DETAILED",
                        "SAMPLE_TYPE_DETAILED",
                        "SEQ_YEAR",
                        "SEQ_YEAR",
                        "SEQ_YEAR",
                    ],
                    "main_genie_field_value": ["P", "Q", "R", "M", "N", "O"],
                    "Main_Genie_Release_Version": ["V1", "V1", "V1", "V1", "V1", "V1"],
                }
            ),
        },
    ]


@pytest.mark.parametrize(
    "test_cases",
    get__update_tier1a_data_replacement_mapping_table_test_cases(),
    ids=lambda x: x["name"],
)
def test_update_tier1a_data_replacement_mapping_table(
    syn, table_schema, test_cases, config
):
    with patch.object(syn, "get", return_value=table_schema) as patch_get, patch.object(
        syn, "tableQuery"
    ) as patch_table_query, patch.object(syn, "store") as patch_store, patch.object(
        syn, "delete"
    ) as patch_delete, patch(
        "utilities.update_version"
    ) as update_version:
        patch_table_query.return_value = MagicMock(etag="test_etag")
        logger = MagicMock(spec=logging.Logger)

        comment = "test comment"
        # Call the function
        update_tier1a_data_replacement_mapping_table(
            syn,
            test_cases["merged_table"],
            test_cases["form"],
            config,
            comment=comment,
            logger=logger,
            bpc_column_list=test_cases["bpc_column_list"],
            main_genie_column_list=test_cases["main_genie_column_list"],
            cohort=test_cases["cohort"],
        )

        # Validate
        patch_get.assert_called_with(
            config["tier1a_replacement_mapping"][
                f"{test_cases['form']}_tier1a_replacement_mapping_table"
            ]
        )
        if test_cases["cohort"]:
            patch_table_query.assert_called_with(
                f"SELECT * FROM {table_schema.id} where cohort = '{test_cases['cohort']}'"
            )
        else:
            patch_table_query.assert_called_with(f"SELECT * FROM {table_schema.id}")
        patch_delete.assert_called_once()
        args, kwargs = patch_store.call_args
        stored_table = args[0]
        assert stored_table.schema == table_schema
        pd.testing.assert_frame_equal(
            stored_table.asDataFrame(), test_cases["expected_df"]
        )
        logger.info.assert_called_with(
            "Updating version for tier1a data replacement mapping table"
        )
        update_version.assert_called_with(
            syn,
            table_schema.id,
            f"{comment}_mainGENIE_{config['main_genie_release_version']}",
        )


def test_extract_site_name_from_sample_id_single_str():
    cpt_genie_sample_id = "GENIE-SAGE-1"
    assert extract_site_name_from_sample_id(cpt_genie_sample_id) == "SAGE"


@pytest.mark.parametrize(
    "input_df,expected_df",
    [
        (
            pd.DataFrame(
                {
                    "cohort": ["A", "A"],
                    "cpt_sample_id": [
                        "cohortA-site1-sample01",
                        "cohortA-site1-sample02",
                    ],
                    "bpc_col1": [10, 20],
                    "bpc_col2": [30, 40],
                    "genie_col1": [50, 60],
                    "genie_col2": [70, 80],
                }
            ),
            pd.DataFrame(
                {
                    "cohort": ["A", "A", "A", "A"],
                    "cpt_sample_id": [
                        "cohortA-site1-sample01",
                        "cohortA-site1-sample02",
                        "cohortA-site1-sample01",
                        "cohortA-site1-sample02",
                    ],
                    "bpc_field": ["bpc_col1", "bpc_col1", "bpc_col2", "bpc_col2"],
                    "bpc_field_value": [10, 20, 30, 40],
                    "main_genie_field": [
                        "genie_col1",
                        "genie_col1",
                        "genie_col2",
                        "genie_col2",
                    ],
                    "main_genie_field_value": [50, 60, 70, 80],
                }
            ),
        ),
        (
            pd.DataFrame(
                {
                    "cohort": ["A", "A"],
                    "cpt_sample_id": [
                        "cohortA-site1-sample01",
                        "cohortA-site1-sample02",
                    ],
                    "bpc_col1": [10, 20],
                    "bpc_col2": [30, np.nan],
                    "genie_col1": [np.nan, 60],
                    "genie_col2": [70, 80],
                }
            ),
            pd.DataFrame(
                {
                    "cohort": ["A", "A", "A", "A"],
                    "cpt_sample_id": [
                        "cohortA-site1-sample01",
                        "cohortA-site1-sample02",
                        "cohortA-site1-sample01",
                        "cohortA-site1-sample02",
                    ],
                    "bpc_field": ["bpc_col1", "bpc_col1", "bpc_col2", "bpc_col2"],
                    "bpc_field_value": [10, 20, 30, np.nan],
                    "main_genie_field": [
                        "genie_col1",
                        "genie_col1",
                        "genie_col2",
                        "genie_col2",
                    ],
                    "main_genie_field_value": [np.nan, 60, 70, 80],
                }
            ),
        ),
    ],
    ids=["dataframe_without_NAs", "dataframe_with_NAs"],
)
def test_convert_tier1a_data_replacement_mapping_table_to_long(input_df, expected_df):
    # Define input parameters
    id_vars = ["cohort", "cpt_sample_id"]
    bpc_column_list = ["bpc_col1", "bpc_col2"]
    main_genie_column_list = ["genie_col1", "genie_col2"]

    # Call the function
    result_df = convert_tier1a_data_replacement_mapping_table_to_long(
        input_df, id_vars, bpc_column_list, main_genie_column_list
    )
    # Assert that the result matches the expected DataFrame
    pd.testing.assert_frame_equal(
        result_df.reset_index(drop=True), expected_df.reset_index(drop=True)
    )
