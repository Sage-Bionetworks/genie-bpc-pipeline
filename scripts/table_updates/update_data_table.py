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
import math

import pandas
import numpy

from utilities import *

TABLE_INFO = {"primary": ('syn23285911',"table_type='data'"),
              "irr": ('syn21446696',"table_type='data' and double_curated is true")}

def _store_data(syn, table_id, label_data, table_type, logger, dry_run):
    table_schema = syn.get(table_id)
    logger.info(f"Updating table: {table_schema.name} {table_id}")
    form_label = table_schema.form_label[0]
    table_columns = syn.getColumns(table_schema.columnIds)
    table_columns = [col['name'] for col in list(table_columns)]
    table_columns = list(set(table_columns) & set(label_data.columns)) # variable in dd but not in data
    table_columns.append("redcap_repeat_instrument")
    temp_data = label_data[table_columns]
    if form_label == "non-repeating":
        temp_data = temp_data[temp_data.redcap_repeat_instrument.isnull()]
    else:
        temp_data = temp_data[temp_data.redcap_repeat_instrument==form_label]
    temp_data.drop(columns="redcap_repeat_instrument",inplace=True)
    # remove rows with no data
    cols_to_skip =['cohort','record_id','redcap_data_access_group']
    if "redcap_repeat_instance" in table_columns:
        if table_schema.form in [['prissmm_pathology'],['ca_directed_radtx'],['cancer_diagnosis']]:
            cols_to_skip = ['cohort','record_id','redcap_repeat_instance']
        else:
            cols_to_skip.append("redcap_repeat_instance")
    rows_to_drop = temp_data.index[temp_data.apply(lambda row: check_empty_row(row,cols_to_skip),axis=1)]
    temp_data.drop(index=rows_to_drop,inplace=True)
    # remove .0 from all columns
    temp_data = temp_data.applymap(lambda x: float_to_int(x))
    # update table
    table_query = syn.tableQuery("SELECT * from %s" % table_id)
    if table_type=='irr':
        # check for exsiting id to update for new data only
        existing_records = list(set(table_query.asDataFrame()['record_id']))
        temp_data = temp_data[~temp_data['record_id'].isin(existing_records)]
    if not dry_run:
        if table_type=='primary':
            table = syn.delete(table_query.asRowSet()) # wipe the table
        table = syn.store(Table(table_schema, temp_data))
    else:
        temp_data.to_csv(table_id+"_temp.csv")

def store_data(syn, master_table, label_data, table_type, logger, dry_run):
    logger.info("Updating data for %s tables..." % table_type)
    for table_id in master_table['id']:
        _store_data(syn, table_id, label_data, table_type, logger, dry_run)

def get_phi_cutoff(unit):
    switcher = {
        "day": math.floor(89*365),
        "month": math.floor(89*12),
        "year": 89
    }
    return(switcher.get(unit, "Invalid unit"))

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
    col_int = pandas.to_numeric(df_col, errors='coerce')
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
    if len(df_col_dt_compare)==0:
        df_col_dt_compare = datetime.date.today()
    else:
        df_col_dt_compare = df_col_dt_compare.fillna(datetime.datetime.now().strftime("%Y-%m-%d"))  
        df_col_dt_compare = df_col_dt_compare.apply(lambda x: datetime.datetime.strptime(str(x),"%Y-%m-%d").date())
    dates_diff = df_col_dt_compare - df_col_birth_year.apply(lambda x: datetime.date(datetime.date.today().year, 1, 1) if math.isnan(x) else datetime.date(int(x), 1, 1))
    date_diff_bool = dates_diff.dt.days > get_phi_cutoff("day")
    vital_status_bool = df_col_vital_status == 'No'
    to_redact = date_diff_bool & vital_status_bool
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
    col_int = pandas.to_numeric(df_col_seq_age, errors='coerce')
    col_seq_age = (col_int > get_phi_cutoff("day")) | contain_greaterthan
    vital_status_bool = df_col_vital_status == 'Yes'
    to_redact = col_seq_age & vital_status_bool
    return to_redact[to_redact].index

def _redact_table(df, interval_cols_info):
    record_to_redact = list()
    interval_list = list(set(interval_cols_info['variable']).intersection(set(df.columns.values.tolist())))
    if len(interval_list) != 0:
        for col in interval_list:
            unit = interval_cols_info.loc[interval_cols_info['variable']==col,'unit'].values[0]
            to_redact = _to_redact_interval(df[col],unit)
            index_to_redact = to_redact.index[to_redact==True]
            df.loc[index_to_redact, col] = ""
            df[col] = df[col].map(float_to_int)
            record_to_redact = record_to_redact+[df['record_id'][x] for x in index_to_redact]
    return df, record_to_redact

def update_redact_table(syn, redacted_table_info, full_data_table_info, logger):
    interval_cols_info = download_synapse_table(syn,'syn23281483','')
    # Create new master table
    master_table = redacted_table_info.merge(full_data_table_info, on='name', suffixes=('_redacted','_full'))
    # Get the tables for checking
    curation_table_id = master_table.loc[master_table['name']=="Curation and QA",'id_full'].values[0]
    patient_table_id = master_table.loc[master_table['name']=="Patient Characteristics",'id_full'].values[0]
    sample_table_id = master_table.loc[master_table['name']=="Cancer Panel Test",'id_full'].values[0]
    curation_info = syn.tableQuery('SELECT record_id, curation_dt FROM %s' % curation_table_id).asDataFrame()
    patient_info = syn.tableQuery('SELECT record_id, birth_year, hybrid_death_ind FROM %s' % patient_table_id).asDataFrame()
    sample_info = syn.tableQuery('SELECT record_id, cpt_genie_sample_id, age_at_seq_report FROM %s' % sample_table_id).asDataFrame()
    patient_curation_info = patient_info.merge(curation_info, how='left', on='record_id')
    clinical_info = patient_info.merge(sample_info, how='right', on='record_id')
    # Check birth year vs curation date with vital status = alive
    birth_year_flag = _to_redact_birth_year(patient_curation_info['birth_year'], patient_curation_info['curation_dt'], patient_curation_info['hybrid_death_ind'])
    record_to_redact = patient_curation_info.loc[birth_year_flag, 'record_id'].values.tolist()
    # Check seq age with vital status = deceased
    seq_age_flag = _to_redact_seq_age(clinical_info['age_at_seq_report'], clinical_info['hybrid_death_ind'])
    record_to_redact = record_to_redact+clinical_info.loc[seq_age_flag, 'record_id'].values.tolist()
    # Check interval fields and store the data table
    for _,row in master_table.iterrows():
        if row['name'] != 'Patient Characteristics':
            table_id = row['id_full']
            df = syn.tableQuery('SELECT * FROM %s' % table_id).asDataFrame()
            new_df, new_record_to_redact = _redact_table(df, interval_cols_info)
            new_df.reset_index(drop=True, inplace=True)
            record_to_redact = record_to_redact+new_record_to_redact
            table_schema = syn.get(row['id_redacted'])
            logger.info("Updating table: %s" % table_schema.name)
            table_query = syn.tableQuery("SELECT * from %s" % row['id_redacted'])
            table = syn.delete(table_query.asRowSet()) # wipe the table
            table = syn.store(Table(table_schema, new_df))
    # Modify patient table
    df = syn.tableQuery('SELECT * FROM %s' % patient_table_id).asDataFrame()
    new_df, new_record_to_redact = _redact_table(df, interval_cols_info)
    new_df.reset_index(drop=True, inplace=True)
    record_to_redact = record_to_redact+new_record_to_redact
    # Update the patient table according to redacted records
    logger.info("Updating patient table...")
    final_record = list(set(record_to_redact))
    new_df.loc[new_df['record_id'].isin(final_record), 'redacted'] = 'Yes'
    new_df.loc[new_df['record_id'].isin(final_record), 'birth_year'] = ''
    new_df['birth_year'] = new_df['birth_year'].map(float_to_int)
    new_df['redacted'] = new_df['redacted'].fillna(value='No')
    redacted_patient_id = master_table.loc[master_table['name']=="Patient Characteristics",'id_redacted'].values[0]
    table_schema = syn.get(redacted_patient_id)
    table_query = syn.tableQuery("SELECT * from %s" % redacted_patient_id)
    table = syn.delete(table_query.asRowSet()) # wipe the table
    table = syn.store(Table(table_schema, new_df))
    # Update redacted column in full data patient table
    logger.info("Updating redacted column in the internal table...")
    full_pt_id = master_table.loc[master_table['name']=="Patient Characteristics",'id_full'].values[0]
    full_pt_schema = syn.get(full_pt_id)
    pt_dat_query = syn.tableQuery('SELECT cohort, record_id FROM %s' % full_pt_id)
    pt_dat = pt_dat_query.asDataFrame()
    pt_dat.index = pt_dat.index.map(str)
    pt_dat['index'] = pt_dat.index
    info_to_update = new_df[['cohort','record_id','redacted']]
    result = pandas.merge(pt_dat,info_to_update, on=['cohort','record_id'])
    result.index = result['index']
    result = result[['redacted']]
    syn.store(Table(full_pt_schema, result, etag=pt_dat_query.etag))

def custom_fix(syn, master_table, logger):
    logger.info("Custom fix in progress...")
    # Modify the cpt_seq_date table per request
    cpt_table_id = master_table.loc[master_table['form_label']=="Cancer Panel Test",'id'].values[0]
    cpt_table_schema = syn.get(cpt_table_id)
    cpt_dat_query = syn.tableQuery('SELECT cpt_genie_sample_id FROM %s' % cpt_table_id)
    cpt_dat = cpt_dat_query.asDataFrame()
    cpt_dat.index = cpt_dat.index.map(str)
    cpt_dat['index'] = cpt_dat.index
    genie_sample_dat = syn.tableQuery('SELECT SAMPLE_ID, SEQ_YEAR FROM syn7517674').asDataFrame()
    cpt_seq_dat = cpt_dat.merge(genie_sample_dat, how='left',left_on='cpt_genie_sample_id', right_on='SAMPLE_ID')
    cpt_seq_dat.index = cpt_seq_dat['index']
    cpt_seq_dat = cpt_seq_dat[['SEQ_YEAR']]
    cpt_seq_dat.columns = ['cpt_seq_date']
    cpt_seq_dat['cpt_seq_date'] = cpt_seq_dat['cpt_seq_date'].map(float_to_int)
    syn.store(Table(cpt_table_schema, cpt_seq_dat, etag=cpt_dat_query.etag))
    # Modify the cpt_sample_type -> map to text value
    cpt_table_schema = syn.get(cpt_table_id)
    cpt_dat_query = syn.tableQuery('SELECT cpt_sample_type FROM %s WHERE cpt_sample_type in (1,2,3,4,5,6,7)' 
                                   % cpt_table_id)
    cpt_dat = cpt_dat_query.asDataFrame()
    cpt_dat['cpt_sample_type'] = pandas.to_numeric(cpt_dat['cpt_sample_type'])
    sample_type_mapping = syn.tableQuery("SELECT * FROM syn7434273").asDataFrame()
    sample_type_mapping_dict = sample_type_mapping.set_index('CODE').to_dict()['DESCRIPTION']
    cpt_dat['cpt_sample_type'] = cpt_dat['cpt_sample_type'].map(sample_type_mapping_dict)
    syn.store(Table(cpt_table_schema, cpt_dat, etag=cpt_dat_query.etag))
    logger.info("Completed")

def main():
    #add arguments
    parser = argparse.ArgumentParser(
        description='Update data tables on Synapse for BPC databases')
    parser.add_argument(
        "table", 
        type=str,
        help='Specify table type to run',
        choices=TABLE_INFO.keys())
    parser.add_argument(
        "-s", "--synapse_config",
        default=synapseclient.client.CONFIG_FILE,
        help="Synapse credentials file")
    parser.add_argument(
        "-p", "--project_config",
        default="config.json",
        help="Project config file")
    parser.add_argument(
        "-m","--message",
        default="",
        help = "Version comment")
    parser.add_argument(
        "-d", "--dry_run",
        action="store_true",
        help="dry run flag"
    )

    args = parser.parse_args()
    table_type = args.table
    synapse_config = args.synapse_config
    project_config = args.project_config
    comment = args.message
    dry_run = args.dry_run
    
    #login to synapse
    syn = synapse_login(synapse_config)
    
    #create logger
    logger_name = "testing" if dry_run else "production"
    logger = setup_custom_logger(logger_name)
    logger.info('Updating data tables on Synapse!')

    #read the project config file
    with open(project_config) as config_file:
        cohort_info = json.load(config_file)
        logger.info("Read cohort information successful.")
        config_file.close()
    
    # get master table
    table_id, condition = list(TABLE_INFO[table_type])
    master_table = download_synapse_table(syn, table_id, condition)
    TABLE_INFO["redacted"] = ('syn21446696',"table_type='data' and double_curated is false")
    
   # download data files 
   # TODO: find the cohort that has new data
    cohort_info_selected = cohort_info[table_type]
    cohort_data_list = []
    for cohort in cohort_info_selected:
        df = get_data(syn, cohort_info_selected[cohort],cohort)
        cohort_data_list.append(df)
    label_data = pandas.concat(cohort_data_list, axis=0, ignore_index=True)
    label_data['redacted'] = numpy.nan
    
    # update data tables
    store_data(syn, master_table, label_data, table_type, logger, dry_run)
    if not dry_run:
        custom_fix(syn, master_table, logger)
        if table_type == 'primary':
            table_id, condition = list(TABLE_INFO['redacted'])
            redacted_table_info = download_synapse_table(syn, table_id, condition)
            logger.info("Updating redacted tables...")
            update_redact_table(syn, redacted_table_info, master_table, logger)
            logger.info("Updating version for redacted tables")
            for table_id in redacted_table_info['id']:
                update_version(syn, table_id, comment)
        logger.info("Updating version for %s tables" % table_type)
        for table_id in master_table['id']:
            update_version(syn, table_id, comment)
        logger.info("Table update is completed!")

if __name__ == "__main__":
    main()
