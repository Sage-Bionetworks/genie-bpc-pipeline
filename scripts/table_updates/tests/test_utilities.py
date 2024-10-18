import pdb
from unittest.mock import MagicMock, create_autospec, patch

import numpy as np
import pandas as pd
import pytest
import synapseclient
from synapseclient import Schema, Table
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