# !/usr/bin/python
"""BPC Syapse Table Update

This script updates the primary case tables and irr case tables for BPC.
The primary case tables contains two sets: the curated tables 
and redacted tables.
The curated tables are located in the Sage Internal project and 
the redacted tables are in the BPC internal project.

Usage:
To update the primary case tables
python update_table_update.py -m [version_message] primary
To update the irr case tables
python update_table_update.py -m [version_message] irr
"""

import argparse
import datetime
import json
import logging
import math
import sys
from typing import List

import numpy
import pandas
import synapseclient
import utilities
from synapseclient import Schema, Table

TABLES = {
    "production": {
        "primary": ("syn23285911", "table_type='data'"),
        "irr": ("syn21446696", "table_type='data' and double_curated is true"),
        "redacted": ("syn21446696", "table_type='data' and double_curated is false"),
    },
    "staging": {
        "primary": ("syn63616766", "table_type='data'"),
        "redacted": ("syn63617582", "table_type='data' and double_curated is false"),
    },
}


def get_main_genie_clinical_file(
    syn: synapseclient.Synapse,
    release: str,
    release_files_table_synid: str,
    form: str, 
    column_mapping_table: pandas.DataFrame,
    logger: logging.Logger = None,
) -> pandas.DataFrame:
    """This retrieves the main genie clinical patient or sample file from consortium release

    Args:
        syn (synapseclient.Synapse): synapse client connection
        release (str): release version to pull from for main genie
        release_files_table_synid (str): synapse id of the data relese files table
        from main genie
        form (str): form name, can be either patient_characteristics or cancer_panel_test
        column_mapping_table (pandas.DataFrame): GENIE BPC Elements Mapping table
        logger (logging.Logger): custom logger. Optional.

    Returns:
        pandas.DataFrame: the read in clinical file as dataframe
    """
    release_files = utilities.download_synapse_table(syn, release_files_table_synid)
    if form == "patient_characteristics": 
        clinical_link_synid = release_files[
            (release_files["release"] == release)
            & (release_files["name"] == "data_clinical_patient.txt")
            ]["fileSynId"].values[0]
    else: 
        clinical_link_synid = release_files[
            (release_files["release"] == release)
            & (release_files["name"] == "data_clinical_sample.txt") 
            ]["fileSynId"].values[0]
    clinical_ent = syn.get(clinical_link_synid, followLink=True)
    clinical_df = pandas.read_csv(clinical_ent.path, sep="\t", skiprows=4)
    # get column list for the form
    column_list = column_mapping_table.loc[column_mapping_table['prissmm_form']== form, ].genie_element.to_list()
    assert (
        not clinical_df.empty
    ), f"Clinical file pulled from {clinical_link_synid} link is empty."
    assert set(column_list) < set(clinical_df.columns), (
        f"Clinical file pulled from {clinical_link_synid} link is missing an expected column. \\n"
        f"Expected columns: {column_list}"
    )
    if logger:
        logger.info(f"CLINICAL_FILE_LINK:{clinical_link_synid}")
        logger.info(f"RELEASE_FILES_TABLE_SYNID:{release_files_table_synid}")
    return clinical_df


def _store_data(syn: synapseclient.Synapse, table_id: str, label_data: pandas.DataFrame, table_type: str, cohort: str, logger: logging.Logger, dry_run: bool):
    """Helper function to store data to each table in the master table.

    Before uploading data to the Synapse table, the provided label data is filtered 
    based on matching columns between the label data and the table schema, as well 
    as the form_label. Data cleansing, including the removal of rows with no data 
    and the conversion of numeric values to integers, is applied to the label data.

    When table_type is set to 'primary', existing data for the cohort is wiped, and 
    new data is inserted. When table_type is set to 'irr', only records that do not 
    already exist in the table are added. The dry_run flag can be used to toggle 
    between uploading the table to Synapse or saving it locally.

    Args:
        syn (synapseclient.Synapse): Synapse client connection
        table_id (string): The table id
        label_data (pandas.DataFrame): The uploaded data
        table_type (string): Table type, primary or irr
        cohort (string): Cohort name
        logger (logging.Logger): The custom logger. Optional.
        dry_run (bool): The dry run flag. If True, perform a dry run.
    """
    table_schema = syn.get(table_id)
    logger.info(f"Updating table: {table_schema.name} {table_id}")
    # subset columns for the uploaded data
    form_label = table_schema.form_label[0]
    table_columns = syn.getColumns(table_schema.columnIds)
    table_columns = [col["name"] for col in list(table_columns)]
    table_columns = list(
        set(table_columns) & set(label_data.columns)
    )  # variable in dd but not in data
    table_columns.append("redcap_repeat_instrument")
    temp_data = label_data[table_columns]
    # subset the uploaded data based on form_label
    if form_label == "non-repeating":
        temp_data = temp_data[temp_data.redcap_repeat_instrument.isnull()]
    else:
        temp_data = temp_data[temp_data.redcap_repeat_instrument == form_label]
    temp_data.drop(columns="redcap_repeat_instrument", inplace=True)
    # remove rows with no data
    cols_to_skip = ["cohort", "record_id", "redcap_data_access_group"]
    if "redcap_repeat_instance" in table_columns:
        if table_schema.form in [
            ["prissmm_pathology"],
            ["ca_directed_radtx"],
            ["cancer_diagnosis"],
        ]:
            cols_to_skip = ["cohort", "record_id", "redcap_repeat_instance"]
        else:
            cols_to_skip.append("redcap_repeat_instance")
    rows_to_drop = temp_data.index[
        temp_data.apply(lambda row: utilities.check_empty_row(row, cols_to_skip), axis=1)
    ]
    temp_data.drop(index=rows_to_drop, inplace=True)
    # remove .0 from all columns
    temp_data = temp_data.applymap(lambda x: utilities.float_to_int(x))
    # update table
    table_query = syn.tableQuery(
        f"SELECT * FROM {table_schema.id} where cohort = '{cohort}'"
    )
    if table_type == "irr":
        # check for exsiting id to update for new data only
        existing_records = list(set(table_query.asDataFrame()["record_id"]))
        temp_data = temp_data[~temp_data["record_id"].isin(existing_records)]
    if not dry_run:
        if table_type == "primary":
            table = syn.delete(table_query)  # wipe the cohort data
        table = syn.store(Table(table_schema, temp_data))
    else:
        temp_data.to_csv(table_id + "_temp.csv")


def store_data(syn: synapseclient.Synapse, master_table: pandas.DataFrame, label_data: pandas.DataFrame, table_type: str, cohort: str, logger: logging.Logger, dry_run: bool):
    """Store data to each table in the master table.

    Args:
        syn (synapseclient.Synapse): Synapse client connection
        master_table (pandas.DataFrame): Table of all of the primary or irr BPC tables
        label_data (pandas.DataFrame): The uploaded data
        table_type (string): Table type, primary or irr
        cohort (string): Cohort name
        logger (logging.Logger): The custom logger. Optional.
        dry_run (bool): The dry run flag. If True, perform a dry run.
    """
    logger.info("Updating data for %s tables..." % table_type)
    for table_id in master_table["id"]:
        _store_data(syn, table_id, label_data, table_type, cohort, logger, dry_run)

def get_phi_cutoff(unit):
    switcher = {"day": math.floor(89 * 365), "month": math.floor(89 * 12), "year": 89}
    return switcher.get(unit, "Invalid unit")


def _to_redact_interval(df_col, unit):
    """Determines interval values that are >89 that need to be redacted
    Returns bool because BIRTH_YEAR needs to be redacted as well based
    on the results

    Args:
        df_col: Dataframe column/pandas.Series of an interval column

    Returns:
        tuple: pandas.Series: to redact boolean vector
    """
    # Some centers pre-redact their values by adding >. These
    # must be redacted
    contain_greaterthan = df_col.astype(str).str.contains(">", na=False)
    # Add in errors='coerce' to turn strings into NaN
    col_int = pandas.to_numeric(df_col, errors="coerce")
    to_redact = (col_int > get_phi_cutoff(unit)) | contain_greaterthan
    return to_redact


def _to_redact_birth_year(df_col_birth_year, df_col_dt_compare, df_col_vital_status):
    """Determines age > 89 in days as of the date to compare and vital status is alive
    that need to be redacted. If data to compare is NA, default to today.

    Args:
        df_col_birth_year: Dataframe column/pandas.Series of a year column
        df_col_dt_compare: Dataframe column/pandas.Series of a date column
        df_col_vital_status: Dataframe column/pandas.Series of a boolean column

    Returns:
        tuple: pandas.core.indexes: to redact index in Dataframe
    """
    if len(df_col_dt_compare) == 0:
        df_col_dt_compare = datetime.date.today()
    else:
        df_col_dt_compare = df_col_dt_compare.fillna(
            datetime.datetime.now().strftime("%Y-%m-%d")
        )
        df_col_dt_compare = df_col_dt_compare.apply(
            lambda x: datetime.datetime.strptime(str(x), "%Y-%m-%d").date()
        )
    df_col_birth_year = df_col_birth_year.fillna(1900)  # arbitrary year > 89 yrs old
    df_col_birth_date = df_col_birth_year.apply(lambda x: datetime.date(int(x), 1, 1))
    dates_diff = df_col_dt_compare - df_col_birth_date
    dates_phi_bool = dates_diff.map(lambda x: x.days > get_phi_cutoff("day"))
    vital_status_bool = df_col_vital_status == "No"
    to_redact = dates_phi_bool & vital_status_bool
    return to_redact[to_redact].index


def _to_redact_seq_age(df_col_seq_age, df_col_vital_status):
    """
    Determines age of sequencing > 89 in days and vital status is dead
    that need to be redacted.
    Args:
        df_col_seq_age: Dataframe column/pandas.Series of anx interval column
        df_col_vital_status: Dataframe column/pandas.Series of a boolean column
    Returns:
        tuple: pandas.core.indexes: to redact index in Dataframe
    """
    contain_greaterthan = df_col_seq_age.astype(str).str.contains(">", na=False)
    col_int = pandas.to_numeric(df_col_seq_age, errors="coerce")
    col_seq_age = (col_int > get_phi_cutoff("day")) | contain_greaterthan
    vital_status_bool = df_col_vital_status == "Yes"
    to_redact = col_seq_age & vital_status_bool
    return to_redact[to_redact].index


def _redact_table(df, interval_cols_info):
    record_to_redact = list()
    interval_list = list(
        set(interval_cols_info["variable"]).intersection(
            set(df.columns.values.tolist())
        )
    )
    if len(interval_list) != 0:
        for col in interval_list:
            unit = interval_cols_info.loc[
                interval_cols_info["variable"] == col, "unit"
            ].values[0]
            to_redact = _to_redact_interval(df[col], unit)
            index_to_redact = to_redact.index[to_redact == True]
            df.loc[index_to_redact, col] = ""
            df[col] = df[col].map(utilities.float_to_int)
            record_to_redact = record_to_redact + [
                df["record_id"][x] for x in index_to_redact
            ]
    return df, record_to_redact


def update_redact_table(syn: synapseclient.Synapse, redacted_table_info: pandas.DataFrame, full_data_table_info: pandas.DataFrame, cohort: str, logger: logging.Logger):
    """Update redacted table

    Before uploading data to the Synapse table, records are identified for redaction 
    based on criteria such as birth year, sequencing age, vital status, and interval 
    fields. The redacted data is then stored in the BPC internal tables.
    
    A special case applies to the Patient Characteristics table: flagged records are 
    updated, with the birth_year field cleared. Additionally, the "redacted" column 
    in this table is updated within the Sage Internal project.
    
    Args:
        syn (synapseclient.Synapse): Synapse client connection
        redacted_table_info (pandas.DataFrame): Table of all of the redacted tables
        full_data_table_info (pandas.DataFrame): Table of all of the primary or irr BPC tables
        cohort (string): Cohort name
        logger (logging.Logger): The custom logger. Optional.
    """
    interval_cols_info = utilities.download_synapse_table(syn, "syn23281483")
    # Create new master table
    master_table = redacted_table_info.merge(
        full_data_table_info, on="name", suffixes=("_redacted", "_full")
    )
    # Get the tables for checking
    curation_table_id = master_table.loc[
        master_table["name"] == "Curation and QA", "id_full"
    ].values[0]
    patient_table_id = master_table.loc[
        master_table["name"] == "Patient Characteristics", "id_full"
    ].values[0]
    sample_table_id = master_table.loc[
        master_table["name"] == "Cancer Panel Test", "id_full"
    ].values[0]
    # download tables
    condition = f"cohort = '{cohort}'"
    curation_info = utilities.download_synapse_table(syn, curation_table_id, "record_id, curation_dt", condition)
    patient_info = utilities.download_synapse_table(syn, patient_table_id, "record_id, birth_year, hybrid_death_ind", condition)
    sample_info = utilities.download_synapse_table(syn, sample_table_id, "record_id, cpt_genie_sample_id, age_at_seq_report", condition)
    patient_curation_info = patient_info.merge(
        curation_info, how="left", on="record_id"
    )
    clinical_info = patient_info.merge(sample_info, how="right", on="record_id")
    # Check birth year vs curation date with vital status = alive
    birth_year_flag = _to_redact_birth_year(
        patient_curation_info["birth_year"],
        patient_curation_info["curation_dt"],
        patient_curation_info["hybrid_death_ind"],
    )
    record_to_redact = patient_curation_info.loc[
        birth_year_flag, "record_id"
    ].values.tolist()
    # Check seq age with vital status = deceased
    seq_age_flag = _to_redact_seq_age(
        clinical_info["age_at_seq_report"], clinical_info["hybrid_death_ind"]
    )
    record_to_redact = (
        record_to_redact + clinical_info.loc[seq_age_flag, "record_id"].values.tolist()
    )
    # Check interval fields and store the data table
    for _, row in master_table.iterrows():
        if row["name"] != "Patient Characteristics":
            table_id = row["id_full"]
            df = utilities.download_synapse_table(syn, table_id, condition = condition)
            new_df, new_record_to_redact = _redact_table(df, interval_cols_info)
            new_df.reset_index(drop=True, inplace=True)
            record_to_redact = record_to_redact + new_record_to_redact
            table_schema = syn.get(row["id_redacted"])
            logger.info("Updating table: %s" % table_schema.name)
            table_query = syn.tableQuery(
                f"SELECT * from {table_schema.id} where cohort = '{cohort}'"
            )
            table = syn.delete(table_query)  # wipe the table
            table = syn.store(Table(table_schema, new_df))

    # Modify patient table
    df = utilities.download_synapse_table(syn, patient_table_id, condition = condition)
    new_df, new_record_to_redact = _redact_table(df, interval_cols_info)
    new_df.reset_index(drop=True, inplace=True)
    record_to_redact = record_to_redact + new_record_to_redact
    # Update the patient table according to redacted records
    logger.info("Updating patient table...")
    final_record = list(set(record_to_redact))
    new_df.loc[new_df["record_id"].isin(final_record), "redacted"] = "Yes"
    new_df.loc[new_df["record_id"].isin(final_record), "birth_year"] = ""
    new_df["birth_year"] = new_df["birth_year"].map(utilities.float_to_int)
    new_df["redacted"] = new_df["redacted"].fillna(value="No")
    redacted_patient_id = master_table.loc[
        master_table["name"] == "Patient Characteristics", "id_redacted"
    ].values[0]
    table_schema = syn.get(redacted_patient_id)
    table_query = syn.tableQuery(
        f"SELECT * from {redacted_patient_id} where cohort = '{cohort}'"
    )
    table = syn.delete(table_query)  # wipe the table
    table = syn.store(Table(table_schema, new_df))
    # Update redacted column in full data patient table
    logger.info("Updating redacted column in the Sage internal table...")
    full_pt_id = master_table.loc[
        master_table["name"] == "Patient Characteristics", "id_full"
    ].values[0]
    full_pt_schema = syn.get(full_pt_id)
    pt_dat_query = syn.tableQuery(
        f"SELECT cohort, record_id FROM {full_pt_id} where cohort = '{cohort}'"
    )
    pt_dat = utilities.download_synapse_table(syn, full_pt_id, "cohort, record_id", condition = condition)
    pt_dat.index = pt_dat.index.map(str)
    pt_dat["index"] = pt_dat.index
    info_to_update = new_df[["cohort", "record_id", "redacted"]]
    result = pandas.merge(pt_dat, info_to_update, on=["cohort", "record_id"])
    result.index = result["index"]
    result = result[["redacted"]]
    syn.store(Table(full_pt_schema, result, etag=pt_dat_query.etag))


def custom_fix_for_cancer_panel_test_table(
    syn: synapseclient.Synapse,
    master_table: pandas.DataFrame,
    logger: logging.Logger,
    config: dict,
) -> None:
    """
    This overwrites the cpt_seq_date column in the Cancer Panel Test
    table in BPC with the SEQ_DATE column from the main genie clinical sample
    file from a consortium release specified in the config.json

    This also overwrites the cpt_sample_type column with the description
    column from the main genie SAMPLE_TYPE_MAPPING table

    Args:
        syn (synapseclient.Synapse): synapse client connection
        master_table (pandas.DataFrame): table of all of the primary BPC tables
        logger (logging.Logger): logger object
        config (dict): config read in
    """
    logger.info("Overwrite tier1a variables in progress...")
    # load GENIE BPC elements mapping table
    column_mapping_table = utilities.download_synapse_table(syn, "syn20945902")
    genie_patient_dat = get_main_genie_clinical_file(
        syn,
        release=config["main_genie_release_version"],
        release_files_table_synid=config["main_genie_data_release_files"],
        form = "patient_characteristics",
        column_mapping_table=column_mapping_table,
        logger=logger,
    )
    genie_sample_dat = get_main_genie_clinical_file(
        syn,
        release=config["main_genie_release_version"],
        release_files_table_synid=config["main_genie_data_release_files"],
        form = "cancer_panel_test",
        column_mapping_table=column_mapping_table,
        logger=logger,
    )
    # unlist form column in master table
    master_table["form"] = master_table["form"].apply(lambda x: ', '.join(x))
    # modify for patient table
    utilities.overwrite_tier1a(syn, "patient_characteristics", master_table, genie_patient_dat, column_mapping_table, bpc_column_list = ["naaccr_ethnicity_code","naaccr_race_code_primary","naaccr_race_code_secondary","naaccr_race_code_tertiary","naaccr_sex_code"],logger=logger)
    # modify for sample table 
    utilities.overwrite_tier1a(syn, "cancer_panel_test", master_table, genie_sample_dat, column_mapping_table, bpc_column_list = ["cpt_sample_type", "cpt_seq_date"], logger=logger)
    logger.info("Completed")


def main():
    # add arguments
    parser = argparse.ArgumentParser(
        description="Update data tables on Synapse for BPC databases"
    )
    parser.add_argument(
        "table", type=str, help="Specify table type to run", choices=TABLES["production"].keys()
    )
    parser.add_argument(
        "-s",
        "--synapse_config",
        default=synapseclient.client.CONFIG_FILE,
        help="Synapse credentials file",
    )
    parser.add_argument(
        "-p", "--project_config", default="config.json", help="Project config file"
    )
    parser.add_argument(
        "-c",
        "--cohort",
        default="",
        help="Cohort name for which the tables should be updated",
    )
    parser.add_argument(
        "-pd",
        "--production",
        action="store_true",
        help="Save output to production folder",
    )
    parser.add_argument("-m", "--message", default="", help="Version comment")
    parser.add_argument("-d", "--dry_run", action="store_true", help="dry run flag")

    args = parser.parse_args()
    table_type = args.table
    synapse_config = args.synapse_config
    project_config = args.project_config
    cohort = args.cohort
    production = args.production
    comment = args.message
    dry_run = args.dry_run

    # login to synapse
    syn = utilities.synapse_login(synapse_config)

    # create logger
    logger_name = "testing" if dry_run else "production"
    logger = utilities.setup_custom_logger(logger_name)
    logger.info("Updating data tables on Synapse!")

    # read the project config file
    with open(project_config) as config_file:
        config = json.load(config_file)
        logger.info("Read cohort information successful.")

    # get master table
    # This is the internal tables with non redacted
    if production:
        TABLE_INFO = TABLES["production"]
    else:
        TABLE_INFO = TABLES["staging"]
    table_id, condition = list(TABLE_INFO[table_type])
    master_table = utilities.download_synapse_table(syn, table_id, condition = condition)
    # download data files
    # TODO: find the cohort that has new data
    # This is a mapping to all the intake data. e.g: ProstateBPCIntake_data
    # found here: https://www.synapse.org/Synapse:syn23286928
    cohort_info_selected = config[table_type]
    label_data = utilities.get_data(syn, cohort_info_selected[cohort], cohort)
    label_data["redacted"] = numpy.nan

    # update data tables
    store_data(syn, master_table, label_data, table_type, cohort, logger, dry_run)
    if not dry_run:
        custom_fix_for_cancer_panel_test_table(syn, master_table, logger, config)
        if table_type == "primary":
            table_id, condition = list(TABLE_INFO["redacted"])
            redacted_table_info = utilities.download_synapse_table(syn, table_id, condition = condition)
            logger.info("Updating redacted tables...")
            #update_redact_table(syn, redacted_table_info, master_table, cohort, logger)
            logger.info("Updating version for redacted tables")
            for table_id in redacted_table_info["id"]:
                utilities.update_version(syn, table_id, comment)
        logger.info("Updating version for %s tables" % table_type)
        for table_id in master_table["id"]:
            utilities.update_version(syn, table_id, comment)
        logger.info("Table update is completed!")


if __name__ == "__main__":
    main()
