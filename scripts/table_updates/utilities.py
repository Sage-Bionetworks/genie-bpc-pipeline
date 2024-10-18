# !/usr/bin/python
import logging
import sys

import pandas
import synapseclient
from synapseclient import Schema, Table


def _is_float(val):
    """Check if the value is float

    Args:
        val: a value to be checked

    Returns:
        boolean: if the value is float
    """
    try:
        float(val)
        return True
    except ValueError:
        return False


def float_to_int(val):
    """Convert float type to integer if the value is integer

    Args:
        val: a value to be checked

    Returns:
        reformatted value
    """
    if _is_float(val):
        val = float(val)
        if val.is_integer():
            return str(int(val))
    return val


def check_empty_row(row, cols_to_skip):
    """
    Check if the row of data is empty with given columns to skip
    """
    return row.drop(cols_to_skip).isnull().all()


def download_synapse_table(syn, table_id: str, select: str = "*", condition: str = "") -> pandas.DataFrame:
    """Download Synapse Table with the given table ID and condition

    Args:
        syn: Synapse credential
        table_id: Synapse ID of a table
        select: Columns to be selected
        condition: additional condition for querying the table

    Returns:
        A Pandas dataframe of the Synapse table
    """
    if condition:
        condition = " WHERE " + condition
    synapse_table = syn.tableQuery(f"SELECT {select} from {table_id}{condition}")
    na_values = [
        "-1.#IND",
        "1.#QNAN",
        "1.#IND",
        "-1.#QNAN",
        "#N/A N/A",
        "#N/A",
        "N/A",
        "n/a",
        "NA",
        "<NA>",
        "#NA",
        "NULL",
        "null",
        "NaN",
        "-NaN",
        "nan",
        "-nan",
        ""
    ]
    synapse_table = synapse_table.asDataFrame(na_values=na_values, keep_default_na=False)
    return synapse_table


def get_data(syn, label_data_id, cohort):
    """Download csv file from Synapse and add cohort column

    Args:
        syn (Object): Synapse credential
        label_data_id (String): Synapse ID of a csv file
        cohort: cohort value to be added as a column

    Returns:
        Dataframe: label data
    """
    na_values = [
        "-1.#IND",
        "1.#QNAN",
        "1.#IND",
        "-1.#QNAN",
        "#N/A N/A",
        "#N/A",
        "N/A",
        "n/a",
        "NA",
        "<NA>",
        "#NA",
        "NULL",
        "null",
        "NaN",
        "-NaN",
        "nan",
        "-nan",
        ""
    ]
    label_data = pandas.read_csv(
        syn.get(label_data_id).path,
        low_memory=False,
        na_values=na_values,
        keep_default_na=False,
    )
    label_data["cohort"] = cohort
    return label_data


def setup_custom_logger(name):
    """Set up customer logger

    Args:
        name (String): Name of the logger

    Returns:
       logger
    """
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler = logging.FileHandler("log.txt", mode="w")
    handler.setFormatter(formatter)
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.addHandler(screen_handler)
    return logger


def synapse_login(synapse_config):
    """Log into Synapse

    Args:
        synapse_config (String): File path to the Synapse config file

    Returns:
        Synapse object
    """
    try:
        syn = synapseclient.login(silent=True)
    except Exception:
        syn = synapseclient.Synapse(configPath=synapse_config, silent=True)
        syn.login()
    return syn


def update_version(syn, table_id, comment):
    """
    Update the table version with given table ID and comment
    """
    syn.restPOST(
        "/entity/%s/table/snapshot" % table_id,
        body='{"snapshotComment":"%s"}' % comment,
    )


def revert_table_version(syn, table_id):
    """Revert table data to previous version

    Args:
        syn: Synapse object
        table_id (String): Synapse ID of the table
    """
    table_schema = syn.get(table_id)
    table_columns = syn.getColumns(table_schema.columnIds)
    table_columns = [col["name"] for col in list(table_columns)]
    previous_version_num = table_schema.versionNumber - 1
    old_data = syn.tableQuery(
        "SELECT * FROM %s.%s" % (table_id, previous_version_num)
    ).asDataFrame()
    old_data = old_data.reset_index(drop=True)
    table_columns = list(set(table_columns) & set(old_data.columns))
    temp_data = old_data[table_columns]
    table_query = syn.tableQuery("SELECT * from %s" % table_id)
    syn.delete(table_query.asRowSet())
    syn.store(Table(table_schema, temp_data))


def overwrite_tier1a(syn, form, master_table, main_genie_table, column_mapping_table, bpc_column_list, logger) -> None:
    # check the validity of bpc_column_list
    valid_col = column_mapping_table.loc[column_mapping_table["prissmm_form"] == form,].prissmm_element.tolist()
    assert all(item in valid_col for item in bpc_column_list), (f"Invalid bpc_column_list. Column names should be matching {valid_col}.")

    logger.info(f"Overwrite {bpc_column_list} in {form}")
    # load bpc table
    cpt_table_id = master_table.loc[
        master_table["form"] == form, "id"
    ].values[0]
    cpt_table_schema = syn.get(cpt_table_id)
    cpt_dat_query = syn.tableQuery(f"SELECT * FROM {cpt_table_id}")
    cpt_dat = download_synapse_table(syn, cpt_table_id)
    cpt_dat.index = cpt_dat.index.map(str)
    cpt_dat["index"] = cpt_dat.index
    # subset main_genie_table based on bpc_column_list
    main_genie_column_list = [", ".join(column_mapping_table.loc[column_mapping_table["prissmm_element"] == col,].genie_element) for col in bpc_column_list]
    
    if form == "patient_characteristics":
        main_genie_table = main_genie_table[main_genie_column_list + ["PATIENT_ID"]]
        cpt_seq_dat = cpt_dat.merge(
            main_genie_table,
            how="left",
            left_on="genie_patient_id",
            right_on="PATIENT_ID",
        )
    else: 
        main_genie_table = main_genie_table[main_genie_column_list + ["SAMPLE_ID"]]
        cpt_seq_dat = cpt_dat.merge(
            main_genie_table,
            how="left",
            left_on="cpt_genie_sample_id",
            right_on="SAMPLE_ID",
        )
    cpt_seq_dat.index = cpt_seq_dat["index"]
    cpt_seq_dat = cpt_seq_dat[main_genie_column_list]
    cpt_seq_dat.columns = bpc_column_list
    # reformat cpt_seq_date column
    if "cpt_seq_date" in cpt_seq_dat.columns:
        cpt_seq_dat["cpt_seq_date"] = cpt_seq_dat["cpt_seq_date"].map(float_to_int)
    syn.store(Table(cpt_table_schema, cpt_seq_dat, etag=cpt_dat_query.etag))


