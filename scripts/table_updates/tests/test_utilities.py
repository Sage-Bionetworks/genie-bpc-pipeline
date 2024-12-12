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
        name="test_table",
        parent="syn123",
        column_names=["col1", "col2"],
        column_types=["STRING", "INTEGER"],
    )

    return schema


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
