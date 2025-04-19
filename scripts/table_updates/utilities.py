# !/usr/bin/python
import builtins
import logging
import sys
from typing import List, Tuple

import pandas
import synapseclient
from synapseclient import Schema, Table

builtins.na_values = [
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
    "",
]


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


def download_synapse_table(
    syn: synapseclient.Synapse,
    table_id: str,
    select: str = "*",
    condition: str = "",
    na_values: list = builtins.na_values,
) -> pandas.DataFrame:
    """Download Synapse Table with the given table ID and condition

    Args:
        syn: Synapse credential
        table_id: Synapse ID of a table
        select: Columns to be selected. Defaults to all columns.
        condition: Additional condition for querying the table. Defaults to all rows.

    Returns:
        A Pandas dataframe of the Synapse table
    """
    if condition:
        condition = " WHERE " + condition
    synapse_table = syn.tableQuery(f"SELECT {select} from {table_id}{condition}")
    synapse_table = synapse_table.asDataFrame(
        na_values=na_values, keep_default_na=False
    )
    return synapse_table


def get_data(
    syn: synapseclient.Synapse,
    label_data_id: str,
    cohort: str,
    na_values: list = builtins.na_values,
):
    """Download csv file from Synapse and add cohort column

    Args:
        syn (Object): Synapse credential
        label_data_id (String): Synapse ID of a csv file
        cohort: cohort value to be added as a column

    Returns:
        Dataframe: label data
    """
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


def remove_backslash(df: pandas.DataFrame, cols: List[str]) -> pandas.DataFrame:
    """Function to detect and remove unwanted backslashes in columns from a dataframe

    Args:
        df (pandas.DataFrame): A dataframe to check against
        cols (List[str]): The list of columns to be updated

    Returns:
        pandas.DataFrame: A dataframe with unwanted backslashes removed
    """
    # check if the given columns are in the dataframe
    if all(col in df.columns for col in cols):
        df[cols] = df[cols].replace(r"\\", "", regex=True)
        return df
    else:
        raise ValueError("Invalid column list. Not all columns are in the dataframe.")


def extract_site_name_from_id(id: str) -> str:
    """Extract site name from CPT ID column
       Site name is the second substring of the ID string

    Args:
        id (str): genie_patient_id or cpt_genie_sample_id

    Returns:
        str: Site name
    """
    # convert nan to empty string
    id = "" if pandas.isna(id) else str(id)
    if id:
        # Split the ID by "-" and return the second substring if the ID is not empty
        return id.split("-")[1]
    else:
        return id


def convert_tier1a_data_replacement_mapping_table_to_long(
    df: pandas.DataFrame,
    id_vars: List[str],
    bpc_column_list: List[str],
    main_genie_column_list: List[str],
) -> pandas.DataFrame:
    """Convert a the tier1a_data_replacement_mapping_table from wide to long format

    Args:
        df (pandas.DataFrame): The dataframe to be converted from wide to long
        id_vars (List[str]): The list of columns to be used as identifier variables
        bpc_column_list (List[str]): The list of BPC columns
        main_genie_column_list (List[str]): The list of main GENIE columns

    Returns:
        pandas.DataFrame: A long format dataframe
    """
    bpc_df = df.melt(
        id_vars=id_vars,
        value_vars=bpc_column_list,
        var_name="bpc_field",
        value_name="bpc_field_value",
    )
    main_genie_df = df.melt(
        id_vars=id_vars,
        value_vars=main_genie_column_list,
        var_name="main_genie_field",
        value_name="main_genie_field_value",
    )

    # Drop id_vars from the main_genie_df before concat
    main_genie_df = main_genie_df.drop(columns=id_vars)

    # Concatenate horizontally
    merged_df = pandas.concat(
        [bpc_df.reset_index(drop=True), main_genie_df.reset_index(drop=True)], axis=1
    )

    return merged_df


def update_tier1a_data_replacement_mapping_table(
    syn: synapseclient.Synapse,
    merged_table: pandas.DataFrame,
    form: str,
    config: dict,
    comment: str,
    logger: logging.Logger,
    bpc_column_list: List[str],
    main_genie_column_list: List[str],
    cohort: str = "",
):
    """Update tier1a data replacement mapping table

    Args:
        syn (synapseclient.Synapse): Synapse object
        merged_table (pandas.DataFrame): The merged table
        form (str): The form name
        config (dict): config read in
        comment (str): The version comment
        logger (logging.Logger): The logger object
        bpc_column_list (List[str]): The list of BPC columns
        main_genie_column_list (List[str]): The list of main GENIE columns
        cohort (str): The cohort name
    """
    if cohort:
        merged_table = merged_table[merged_table["cohort"] == cohort]

    if form == "patient_characteristics":
        table_schema = syn.get(
            config["tier1a_replacement_mapping"][
                "patient_characteristics_tier1a_replacement_mapping_table"
            ]
        )
        # extract site name from genie_patient_id
        merged_table["site"] = merged_table["genie_patient_id"].apply(
            extract_site_name_from_id
        )
        subset_table = merged_table[
            ["cohort", "site", "genie_patient_id"]
            + bpc_column_list
            + main_genie_column_list
        ]
        id_vars = ["cohort", "genie_patient_id", "site"]

    if form == "cancer_panel_test":
        table_schema = syn.get(
            config["tier1a_replacement_mapping"][
                "cancer_panel_test_tier1a_replacement_mapping_table"
            ]
        )
        # extract site name from cpt_genie_sample_id
        merged_table["site"] = merged_table["cpt_genie_sample_id"].apply(
            extract_site_name_from_id
        )
        subset_table = merged_table[
            ["cohort", "site", "cpt_genie_sample_id"]
            + bpc_column_list
            + main_genie_column_list
        ]

        id_vars = ["cohort", "cpt_genie_sample_id", "site"]
    # convert table from wide to long
    subset_table = convert_tier1a_data_replacement_mapping_table_to_long(
        subset_table, id_vars, bpc_column_list, main_genie_column_list
    )
    # add Main_Genie_Release_Version
    subset_table["Main_Genie_Release_Version"] = config["main_genie_release_version"]

    # save the table to sage internal project
    subset_table.reset_index(drop=True, inplace=True)
    if cohort:
        table_query = syn.tableQuery(
            f"SELECT * FROM {table_schema.id} where cohort = '{cohort}'"
        )
    else:
        table_query = syn.tableQuery(f"SELECT * FROM {table_schema.id}")
    table = syn.delete(table_query)  # wipe the cohort data
    table = syn.store(Table(table_schema, subset_table))
    # update the version
    logger.info("Updating version for tier1a data replacement mapping table")
    update_version(
        syn,
        table_schema.id,
        f"{comment}_mainGENIE_{config['main_genie_release_version']}",
    )
