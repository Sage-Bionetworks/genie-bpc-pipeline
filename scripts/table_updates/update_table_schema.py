# !/usr/bin/python
import argparse
from functools import reduce

import pandas
from synapseclient import Column
from utilities import *


def copy_table_schema(syn, from_table_id, to_table_id):
    """
    Copy table schema from one table to another
    """
    from_table_schema = syn.get(from_table_id)
    to_table_schema = syn.get(to_table_id)
    to_table_schema.columnIds = from_table_schema.columnIds
    return to_table_schema


def create_synapse_column(name, col_type, max_size):
    """
    Create a new Synapse Column with given info. ??? may extend, string. use data element column types
    """
    if col_type in ["INTEGER", "DOUBLE", "LARGETEXT"]:
        new_column = Column(name=name, columnType=col_type)
    else:
        max_size = 500 if pandas.isna(max_size) else int(max_size)
        new_column = Column(name=name, columnType=col_type, maximumSize=max_size)
    return new_column


def _expand_checkbox_vars(row):
    """Expand checkbox variables into multiple columns

    Args:
        row (pd.Series): A row of the data element dataframe

    Returns:
        pandas.DataFrame: A dataframe with expanded checkbox variables
    """
    temp_df_list = []
    if not pandas.isna(row["colLabels"]):
        for label in row["colLabels"].split(","):
            temp_df = {}
            temp_df["col_name"] = row["variable"] + "___" + label
            temp_df["synColType"] = row["synColType"]
            temp_df["synColSize"] = row["synColSize"]
            temp_df["variable"] = row["variable"]
            temp_df_list.append(temp_df)
        return pandas.DataFrame(temp_df_list)


def _get_latest_table(form) -> str:
    """get the latest table for each form

    Args:
        form (tuple): the tuple of the form and the corresponding tables outputed by groupby
        e.g. ["form_name": a dataframe contains basic informatiohn for tables associated with the form, including id, name, etc.]

    Returns:
        string: the table id of the table with the latest numeric suffix
    """
    if form[1].shape[0] == 1:
        return form[1]["id"].values[0]
    else:
        # get the table with latest numeric suffix
        latest_part_number = max(form[1]["name"].str.extract(r"(\d+)$")[0].astype(int))
        latest_table = form[1][form[1]["name"].str.contains(f" {latest_part_number}$")]
        return latest_table["id"].values[0]


def _update_table_schema(syn, form, curated_data_element, logger, dry_run):
    form_name = form[0]
    form_df = form[1]
    form_name_list = form_name.split(", ")
    logger.info("Checking %s" % form_name)
    # get the data frame of variables for the form
    vars_dec = curated_data_element[
        curated_data_element.instrument.isin(form_name_list)
    ]
    # get the data frame of existing columns in the tables
    current_cols_df = pandas.DataFrame()
    for _, row in form_df.iterrows():
        current_cols = syn.getColumns(row["id"])
        current_cols = pandas.DataFrame(current_cols)
        current_cols["table_id"] = row["id"]
        current_cols_df = pandas.concat([current_cols_df, current_cols])
    # get the table id for the newest table
    latest_table_id = _get_latest_table(form)
    # get the column count of latest table
    latest_table_col_ct = current_cols_df.loc[
        current_cols_df.table_id == latest_table_id
    ].shape[0]
    # Compare the data element catalog and the current table columns
    # non-checkbox
    non_check_vars = vars_dec[vars_dec.type != "checkbox"]
    non_check_cols = current_cols_df[~current_cols_df["name"].str.contains("___")]
    # checkbox
    checkbox_vars = vars_dec[vars_dec.type == "checkbox"]
    if len(checkbox_vars) != 0:
        # genereate checkbox columns by combining variable and colLabels, each column is named as variable___label
        checkbox_vars_expanded = pandas.concat(
            list(checkbox_vars.apply(lambda row: _expand_checkbox_vars(row), axis=1)),
            ignore_index=True,
        )
        # existing checkbox columns and generate the variable column for later logger info
        checkbox_cols = current_cols_df[current_cols_df["name"].str.contains("___")]
        checkbox_cols["variable"] = checkbox_cols["name"].str.split("___", expand=True)[
            0
        ]

    # columns to add
    cols_to_add = []
    #  non-checkbox
    # add columns in data catalog but not in the table
    non_check_to_add = list(
        set(non_check_vars["variable"]) - set(non_check_cols["name"])
    )
    if len(non_check_to_add) != 0:
        non_check_to_add_df = non_check_vars[
            non_check_vars.variable.isin(non_check_to_add)
        ]
        # create Column objects for new columns
        non_check_new_cols = list(
            non_check_to_add_df.apply(
                lambda x: create_synapse_column(
                    x["variable"], x["synColType"], x["synColSize"]
                ),
                axis=1,
            )
        )
        cols_to_add = cols_to_add + non_check_new_cols
    logger.info(
        "Number of non-checkbox variables to add %s \n" % len(non_check_to_add)
        + "\n".join(non_check_to_add)
    )
    # TODO: non_check_to_rm
    #  checkbox
    if len(checkbox_vars) != 0:
        # new checkbox variables
        # add columns in data catalog but not in the table
        checkbox_to_add = list(
            set(checkbox_vars["variable"]) - set(checkbox_cols["variable"])
        )
        logger.info(
            "Number of checkbox variables to add: %s \n" % len(checkbox_to_add)
            + "\n".join(checkbox_to_add)
        )
        # new checkbox columns
        checkbox_cols_to_add = checkbox_vars_expanded[
            ~checkbox_vars_expanded.col_name.isin(checkbox_cols["name"])
        ]
        if len(checkbox_cols_to_add) != 0:
            checkbox_new_cols = list(
                checkbox_cols_to_add.apply(
                    lambda x: create_synapse_column(
                        x["col_name"], x["synColType"], x["synColSize"]
                    ),
                    axis=1,
                )
            )
            cols_to_add = cols_to_add + checkbox_new_cols
        logger.info(
            "Number of new checkbox columns to add: %s \n"
            % checkbox_cols_to_add.shape[0]
            + "\n".join(checkbox_cols_to_add["col_name"])
        )

    # columns to update: STRING only
    # TODO: check all columnType
    cols_to_update = {}
    #   non-checkbox
    non_check_str_cols = non_check_cols[non_check_cols.columnType == "STRING"]
    merged_non_check_str = non_check_str_cols.merge(
        non_check_vars, how="left", left_on="name", right_on="variable"
    )
    non_check_str_update = merged_non_check_str.query("maximumSize < synColSize")
    if len(non_check_str_update) != 0:
        for table in non_check_str_update.groupby("table_id"):
            table_id = table[0]
            cols_to_update[table_id] = {}
            cols_to_update[table_id]["new"] = list(
                table[1].apply(
                    lambda x: create_synapse_column(
                        x["variable"], x["synColType"], x["synColSize"]
                    ),
                    axis=1,
                )
            )
            # save column id to "old" column
            cols_to_update[table_id]["old"] = list(table[1]["id"])
    logger.info(
        "Number of non-checkbox columns to update: %s \n"
        % non_check_str_update.shape[0]
        + "\n".join(non_check_str_update["variable"])
    )
    #   checkbox
    if len(checkbox_vars) != 0:
        checkbox_str_cols = checkbox_cols[checkbox_cols.columnType == "STRING"]
        merged_checkbox_str = checkbox_str_cols.merge(
            checkbox_vars_expanded, how="left", left_on="name", right_on="col_name"
        )
        # update string checkbox columns if maximumSize is less than synColSize
        checkbox_str_update = merged_checkbox_str.query("maximumSize < synColSize")
        if len(checkbox_str_update) != 0:
            for table in checkbox_str_update.groupby("table_id"):
                table_id = table[0]
                if table_id not in cols_to_update.keys():
                    cols_to_update[table_id] = {}
                    cols_to_update[table_id]["new"] = list(
                        table[1].apply(
                            lambda x: create_synapse_column(
                                x["name"], x["columnType"], x["synColSize"]
                            ),
                            axis=1,
                        )
                    )
                    cols_to_update[table_id]["old"] = list(table[1]["id"])
                else:
                    # extend the existing lists if table_id is already tracked in non-check section
                    cols_to_update[table_id]["new"] = cols_to_update[table_id][
                        "new"
                    ] + list(
                        table[1].apply(
                            lambda x: create_synapse_column(
                                x["name"], x["columnType"], x["synColSize"]
                            ),
                            axis=1,
                        )
                    )
                    cols_to_update[table_id]["old"] = cols_to_update[table_id][
                        "old"
                    ] + list(table[1]["id"])
        logger.info(
            "Number of checkbox columns to update: %s \n" % checkbox_str_update.shape[0]
            + "\n".join(checkbox_str_update["name"])
        )
    # TODO: columns to remove (IGNORE primary_key)
    if not dry_run:
        if len(cols_to_add) != 0:
            # cols_to_add = syn.createColumns(cols_to_add)
            cols_to_add = [syn.store(i) for i in cols_to_add]
            if latest_table_col_ct + len(cols_to_add) <= 152:
                tbl_schema = syn.get(latest_table_id)
                cols_to_add_id = [col["id"] for col in cols_to_add]
                tbl_schema.columnIds = tbl_schema.columnIds + cols_to_add_id
                tbl_schema = syn.store(tbl_schema)
            else:
                # need a new table due to the maximum column limit of 152
                logger.info("TODO: need to add a new table")
        if len(cols_to_update) != 0:
            for table_id in cols_to_update.keys():
                tbl_schema = syn.get(table_id)
                # extract columns that are not to be updated
                tbl_schema.columnIds = [
                    ele
                    for ele in tbl_schema.columnIds
                    if ele not in cols_to_update[table_id]["old"]
                ]
                # save updated columns to Synapse and get their ids
                cols_to_update_new = [
                    syn.store(i) for i in cols_to_update[table_id]["new"]
                ]
                cols_to_update_new_id = [col["id"] for col in cols_to_update_new]
                tbl_schema.columnIds = tbl_schema.columnIds + cols_to_update_new_id
                tbl_schema = syn.store(tbl_schema)


def update_table_schema(syn, logger, dry_run, TABLE_INFO):
    # get the data elements
    curated_data_element = download_synapse_table(
        syn, table_id=TABLE_INFO["catalog_id"], condition="dataType='curated'"
    )
    curated_data_element = curated_data_element[
        [
            "variable",
            "instrument",
            "type",
            "synColType",
            "synColSize",
            "numCols",
            "colLabels",
        ]
    ]
    # create the master table
    sage_table_view = download_synapse_table(
        syn, table_id=TABLE_INFO["sage"][0], condition=TABLE_INFO["sage"][1]
    )
    sage_table_view.drop(columns="table_type", axis=1, inplace=True)
    bpc_table_view = download_synapse_table(
        syn, table_id=TABLE_INFO["bpc"][0], condition=TABLE_INFO["bpc"][1]
    )
    bpc_table_view = bpc_table_view[["id", "name"]]
    ## comment out irr for now in case it is needed for phase 3
    # irr_table_view = download_synapse_table(
    #     syn, table_id=TABLE_INFO["irr"][0], condition=TABLE_INFO["irr"][1]
    # )
    # irr_table_view = irr_table_view[["id", "name"]]
    # irr_table_view["name"] = irr_table_view["name"].apply(
    #     lambda x: x.replace(" - double curated", "")
    # )
    master_table_view = pandas.merge(
        sage_table_view, bpc_table_view, on="name", suffixes=["", "_bpc"]
    )
    # master_table_view = pandas.merge(
    #     sage_table_view,
    #     pandas.merge(
    #         bpc_table_view, irr_table_view, on="name", suffixes=["_bpc", "_irr"]
    #     ),
    #     on="name",
    # )
    # update table schema for Sage Internal tables. Grouping by form since some forms have multiple tables
    form_groups = master_table_view.groupby("form")
    for form in form_groups:
        _update_table_schema(syn, form, curated_data_element, logger, dry_run)
    # copy the table schema to update the BPC Internal and IRR tables
    if not dry_run:
        logger.info("Updating table schemas for BPC and IRR tables")
        for _, row in master_table_view.iterrows():
            new_bpc_schema = copy_table_schema(syn, row["id"], row["id_bpc"])
            syn.store(new_bpc_schema)
            new_irr_schema = copy_table_schema(syn, row["id"], row["id_irr"])
            syn.store(new_irr_schema)


def main():
    parser = argparse.ArgumentParser(
        description="Update table schema on Synapse Tables for BPC"
    )
    parser.add_argument("--debug", action="store_true", help="Synapse Debug Feature")
    parser.add_argument("-d", "--dry_run", action="store_true", help="dry run flag")
    parser.add_argument(
        "-p",
        "--production",
        action="store_true",
        help="Update production tables, default is staging.",
    )

    args = parser.parse_args()
    dry_run = args.dry_run
    # login to synapse
    syn = synapse_login(debug=args.debug)

    # create logger
    logger_name = "staging" if dry_run else "production"
    logger = setup_custom_logger(logger_name)
    logger.info("Updating BPC Synapse Table schemas!")

    if args.production:
        TABLE_INFO = {
            "catalog_id": "syn21431364",
            "sage": ("syn23285911", "table_type='data'"),
            "bpc": ("syn21446696", "table_type='data' and double_curated is false"),
            "irr": ("syn21446696", "table_type='data' and double_curated is true"),
        }
    else:
        TABLE_INFO = {
            "catalog_id": "syn68893705",
            "sage": ("syn63616766", "table_type='data'"),
            "bpc": ("syn63617582", "table_type='data' and double_curated is false"),
            "irr": ("syn63617582", "table_type='data' and double_curated is true"),
        }

    update_table_schema(syn, logger, dry_run, TABLE_INFO)


if __name__ == "__main__":
    main()
