import argparse
import logging
import pandas as pd
import sys
import synapseclient
from synapseclient import Table

# Synapse Table IDs
RETRACTION_TABLE_IDS = {
    "production": "syn52915299",
    "staging": "syn64369514"
}
PT_RETRACTION_TABLE_ID = "syn25998970"
SAMPLE_RETRACTION_TBL_ID = "syn25779833"
RELEASE_INFO_ID = "syn27628075"
BPC_PT_TABLE_ID = "syn21446700"

def download_synapse_table(syn, table_id, condition=None):
    """Download Synapse Table with the given table ID and condition.
    
    Args:
        syn (Synapse): Synapse client object
        table_id (str): Synapse Table ID
        condition (str): SQL condition for querying the table (optional)
    
    Returns:
        pd.DataFrame: DataFrame containing the queried table data
    """
    condition_str = f" WHERE {condition}" if condition else ""
    query = f"SELECT * FROM {table_id}{condition_str}"
    synapse_table = syn.tableQuery(query)
    return synapse_table.asDataFrame()

def setup_logger(name, log_file='log.txt'):
    """Set up a custom logger to log information to both console and a file.
    
    Args:
        name (str): Logger name
        log_file (str): Path to the log file
    
    Returns:
        logging.Logger: Configured logger object
    """
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler = logging.FileHandler(log_file, mode='w')
    file_handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setFormatter(formatter)
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def synapse_login(synapse_config):
    """Log into Synapse using the provided configuration file.
    
    Args:
        synapse_config (str): Path to the Synapse configuration file
    
    Returns:
        Synapse: Synapse client object
    """
    try:
        syn = synapseclient.login(silent=True)
    except Exception:
        syn = synapseclient.Synapse(configPath=synapse_config, silent=True)
        syn.login()
    return syn

def get_file_id_by_name(syn, folder_id, file_name):
    """Retrieve the file ID for a given file name in a specified Synapse folder.
    
    Args:
        syn (Synapse): Synapse client object
        folder_id (str): Synapse Folder ID
        file_name (str): The name of the file to search for
    
    Returns:
        str: File ID if found
    """
    for file in syn.getChildren(folder_id):
        if file['name'] == file_name:
            return file['id']
    return None

def download_sample_file(syn, file_id):
    """Download the sample file by its Synapse file ID.
    
    Args:
        syn (Synapse): Synapse client object
        file_id (str): Synapse file ID
    
    Returns:
        pd.DataFrame: DataFrame with patient IDs from the sample file
    """
    file_entity = syn.get(file_id, followLink=True)
    return pd.read_csv(file_entity.path, sep='\t', header=None, usecols=[0])

def get_genie_id_list_from_bpc_form(syn, table_id, cohort, id_col_name):
    """Get the list of retracted GENIE ids given table id, cohort, ID column name

    Args:
        syn (Object): Synapse Object
        table_id (str): Synapse Table ID
        cohort (str): name of the cohort
        id_col_name (str): ID column name
        
    Returns:
        list: the list of GENIE IDs
    """
    table = download_synapse_table(syn, table_id)
    cohort_columns = table.columns[table.columns.str.match(cohort)]
    genie_id_list = []
    for col in cohort_columns:
        genie_id_list.extend(table.loc[table[col],id_col_name].dropna().unique().tolist())
    return genie_id_list

def main():
    """Main function to update the retraction table for a specific cohort."""
    parser = argparse.ArgumentParser(description='Update retraction for release table on Synapse for BPC')
    parser.add_argument("-c", "--cohort", help="Cohort to release (e.g., NSCLC, CRC, BrCa, BLADDER)", required=True)
    parser.add_argument("-s", "--synapse_config", default=synapseclient.client.CONFIG_FILE, help="Path to Synapse credentials file")
    parser.add_argument("-pd", "--production", action="store_true", help="Save output to production table")
    parser.add_argument("-m", "--message", default="", help="Version comment for the table update")
    parser.add_argument("-d", "--dry_run", action="store_true", help="Flag for dry run (no updates will be made)")

    args = parser.parse_args()

    # Initialize variables
    cohort = args.cohort
    synapse_config = args.synapse_config
    production = args.production
    comment = args.message
    dry_run = args.dry_run
    
    # Log in to Synapse
    syn = synapse_login(synapse_config)
    
    # Create logger
    logger_name = "dry_run" if dry_run else "production"
    logger = setup_logger(logger_name)
    logger.info('Starting BPC retraction for release update process!')
    
    # Get the table ID
    if production:
        RETRACTION_TABLE_ID = RETRACTION_TABLE_IDS["production"]
    else:
        RETRACTION_TABLE_ID = RETRACTION_TABLE_IDS["staging"]
    
    # Download cohort-specific patient data from BPC table
    bpc_cohort_patient = download_synapse_table(syn, BPC_PT_TABLE_ID, f"cohort like '{cohort}%'")
    cohort_patient_list = list(bpc_cohort_patient['record_id'])
    redacted_patient = bpc_cohort_patient[bpc_cohort_patient['redacted'] == "Yes"]
    redacted_patient_list = list(redacted_patient['record_id'])
    
    # Retrieve release info
    release_info = download_synapse_table(syn, RELEASE_INFO_ID, f"cohort='{cohort}' and current=True")
    
    # Get the main GENIE release version
    main_genie_release_folder = release_info['main_genie_release'].values[0]
    main_genie_release_version = syn.get(main_genie_release_folder).name
    clinical_file_id = get_file_id_by_name(syn, main_genie_release_folder, 'data_clinical_sample.txt')
    clinical_pt_from_sample = download_sample_file(syn, clinical_file_id)
    clinical_pt_from_sample.columns = ['patient_id']
    main_genie_patient_list = list(set(clinical_pt_from_sample.iloc[5:]['patient_id']))
   
    # Load the existing redacted patient list and sample list
    current_redacted = download_synapse_table(syn, RETRACTION_TABLE_ID, f"cohort='{cohort}'")
    current_patient_list = list(current_redacted['patient_id'])
    current_sample_list = list(current_redacted['sample_id'])
    
    # Prepare new redacted patient/sample
    new_retracted_df_list = []

    # Compare redacted patients from BPC
    new_patient_from_bpc = list(set(redacted_patient_list) - set(current_patient_list))
    if new_patient_from_bpc:
        logger.info(f"{len(new_patient_from_bpc)} patients added to retraction table due to 89+")
        new_patient_from_bpc_df = pd.DataFrame({
            'cohort': cohort,
            'patient_id': new_patient_from_bpc,
            'reason': '89+'
        })
        new_retracted_df_list.append(new_patient_from_bpc_df)
    
    # Compare redacted patients from GENIE release
    redacted_from_main = list(set(cohort_patient_list) - set(main_genie_patient_list))
    if redacted_from_main:
        logger.info(f"{len(redacted_from_main)} patients found to be retracted in {main_genie_release_version}")
        new_patient_from_main = list(set(redacted_from_main) - set(current_patient_list))
        logger.info(f"{len(new_patient_from_main)} patients added to retraction table due to main GENIE retraction")
        new_patient_from_main_df = pd.DataFrame({
            'cohort': cohort,
            'patient_id': new_patient_from_main,
            'reason': main_genie_release_version
        })
        new_retracted_df_list.append(new_patient_from_main_df)
        
    # Compare retracted patients from BPC form
    patient_from_form = get_genie_id_list_from_bpc_form(syn, PT_RETRACTION_TABLE_ID, cohort,'record_id')
    new_patient_from_form = list(set(patient_from_form) - set(current_patient_list))
    if new_patient_from_form:
        logger.info(f"{len(new_patient_from_form)} patients added to retraction table; submitted by the sites")
        new_patient_from_bpc_form = pd.DataFrame({
            'cohort': cohort,
            'patient_id': new_patient_from_form,
            'reason': 'retraction form'
        })
        new_retracted_df_list.append(new_patient_from_bpc_form)
    
    # Compare retracted sample from BPC form
    sample_from_form = get_genie_id_list_from_bpc_form(syn, SAMPLE_RETRACTION_TBL_ID, cohort, 'SAMPLE_ID')
    new_sample_from_form = list(set(sample_from_form) - set(current_sample_list))
    if new_sample_from_form:
        logger.info(f"{len(new_sample_from_form)} samples added to retraction table; submitted by the sites")
        new_patient_from_bpc_form = pd.DataFrame({
            'cohort': cohort,
            'sample_id': new_sample_from_form,
            'reason': 'retraction form'
        })
        new_retracted_df_list.append(new_sample_from_form)
    
    new_retracted_df = pd.concat(new_retracted_df_list)
    
    # Final actions based on dry run flag
    if new_retracted_df.empty:
        logger.info(f"No new patient/sample added to the retraction table for {cohort} cohort.")
    else:
        if dry_run:
            logger.info("Dry run: Writing new retracted patients/samples to temp file.")
            new_retracted_df.to_csv("retraction_temp.csv")
        else:
            logger.info(f"Updating retraction table {RETRACTION_TABLE_ID}...")
            table_schema = syn.get(RETRACTION_TABLE_ID)
            table = syn.store(Table(table_schema, new_retracted_df))
            syn.create_snapshot_version(RETRACTION_TABLE_ID, comment=comment)

if __name__ == "__main__":
    main()
