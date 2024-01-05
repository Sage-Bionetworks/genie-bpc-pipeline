"""Update the BPC retraction table by 89+ patients and 
   latest main GENIE consortium release that is 
   tied to the most recent public release
   
   Usage:
   python update_retraction_table.py -c [cohort]] -m [version comment]
"""
import argparse
import logging
import pandas
import sys

import synapseclient

from synapseclient import Schema, Column, Table

RETRACTION_TABLE_ID = "syn52915299"
RELEASE_INFO_ID = "syn27628075"
BPC_PT_TABLE_ID = "syn21446700"

def download_synapse_table(syn, table_id, condition):
    """Download Synapse Table with the given table ID and condition
    
    Args:
        syn: Synapse credential
        table_id: Synapse ID of a table
        condition: additional condition for querying the table
    
    Returns:
        Dataframe: synapse table
    """
    if condition:
        condition = " WHERE "+condition
    synapse_table = syn.tableQuery("SELECT * from %s%s" % (table_id,condition))
    synapse_table = synapse_table.asDataFrame()
    return(synapse_table)

def setup_custom_logger(name):
    """Set up customer logger

    Args:
        name (String): Name of the logger 
    
    Returns:
       logger
    """
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    handler = logging.FileHandler('log.txt', mode='w')
    handler.setFormatter(formatter)
    screen_handler = logging.StreamHandler(stream=sys.stdout)
    screen_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    logger.addHandler(screen_handler)
    return(logger)

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
    return(syn)

def update_version(syn, table_id, comment):
    """
    Update the table version with given table ID and comment
    """
    syn.restPOST("/entity/%s/table/snapshot" % table_id, body='{"snapshotComment":"%s"}' % comment)

def get_file_id_by_name(syn, folder_id, file_name):
    """Get file synapse ID by name while the parent folder is given

    Args:
        syn: Synapse Object
        folder_id (String): Synapse Folder ID
        file_name (String): File Name
    """
    for f in syn.getChildren(folder_id, includeTypes=['file']):
        f_name = f['name']
        f_id = f['id']
        if f_name == file_name:
            return(f_id)

def main():
    parser = argparse.ArgumentParser(
        description='Update retraction for release table on Synapse for BPC')
    parser.add_argument(
        "-c", "--cohort",
        help="Cohort to release. i.e. NSCLC, CRC, BrCa, BLADDER..."
    )
    parser.add_argument(
        "-s", "--synapse_config",
        default=synapseclient.client.CONFIG_FILE,
        help="Synapse credentials file"
    )
    parser.add_argument(
        "-m","--message",
        default="",
        help = "Version comment"
    )
    parser.add_argument(
        "-d", "--dry_run",
        action="store_true",
        help="dry run flag"
    )
    
    args = parser.parse_args()
    cohort = args.cohort
    synapse_config = args.synapse_config
    comment = args.message
    dry_run = args.dry_run
    
    #login to synapse
    syn = synapse_login(synapse_config)
    
    #create logger
    logger_name = "testing" if dry_run else "production"
    logger = setup_custom_logger(logger_name)
    logger.info('Updating BPC retraction for release table on Synapse!')
    
    #read the BPC patient table and get 89+ patients for the cohort
    bpc_cohort_patient = download_synapse_table(syn, BPC_PT_TABLE_ID, "cohort='"+cohort+"'")
    cohort_patient_list = list(bpc_cohort_patient['record_id'])
    redacted_patient = bpc_cohort_patient[bpc_cohort_patient['redacted']=="Yes"]
    redacted_patient_list = list(redacted_patient['record_id'])
    
    #read release info
    release_info = download_synapse_table(syn, RELEASE_INFO_ID, "cohort='"+cohort+"' and current=True")
    
    #load the main GENIE release
    main_genie_release_folder = release_info['main_genie_release'].values[0]
    main_genie_release_version = syn.get(main_genie_release_folder).name
    clinical_file_id = get_file_id_by_name(syn, main_genie_release_folder, 'data_clinical_sample.txt')
    clinical_pt_from_sample = pandas.read_csv(syn.get(clinical_file_id).path, sep='\t', header=None, usecols=[0])
    clinical_pt_from_sample.columns = ['patient_id']
    main_genie_patient_list = list(set(clinical_pt_from_sample.iloc[5:]['patient_id']))
   
    #load the existing redacted patient list
    current_redacted = download_synapse_table(syn, RETRACTION_TABLE_ID, "cohort='"+cohort+"'")
    current_patient_list = list(current_redacted['patient_id'])
    
    new_redacted_df = pandas.DataFrame()
    
    #compare redacted patient between BPC table vs Redaction for Release Table
    new_patient_from_bpc = list(set(redacted_patient_list) - set(current_patient_list))
    if len(new_patient_from_bpc) == 0:
        logger.info('No additional redacted patient is added to the '+cohort)
    else:
        logger.info(str(len(new_patient_from_bpc)) + " patients are added to the retraction table due to 89+")
        new_patient_from_bpc_df = pandas.DataFrame({'cohort':cohort,
                                                    'patient_id':new_patient_from_bpc,
                                                    'reason': '89+'})
        
    #compare redacted patient between main GENIE vs BPC tables
    redacted_from_main = list(set(cohort_patient_list)-set(main_genie_patient_list))
    if len(redacted_from_main) == 0:
        logger.info('No additional redacted patient is found due to main GENIE retraction to the '+cohort)
    else:
        logger.info(str(len(redacted_from_main)) + " patients are found to be retracted in "+main_genie_release_version)
        new_patient_from_main = list(set(redacted_from_main) - set(current_patient_list))
        logger.info(str(len(new_patient_from_main)) + " patients are added to the retraction table due to main GENIE retraction")
        new_patient_from_main_df = pandas.DataFrame({'cohort':cohort,
                                                    'patient_id':new_patient_from_main,
                                                    'reason': main_genie_release_version})
    
    # append the table
    new_retracted_df = pandas.DataFrame()
    if 'new_patient_from_bpc_df' in locals():
        new_retracted_df = new_retracted_df.append(new_patient_from_bpc_df)
    if 'new_patient_from_main_df' in locals():
        new_retracted_df = new_retracted_df.append(new_patient_from_main_df)
    
    if new_retracted_df.empty:
        logger.info('No new patient is added to the retraction for release from BPC and '+main_genie_release_version+" for "+cohort)
    else:
        if dry_run:
            logger.info("Write to a temp file for review")
            new_retracted_df.to_csv("retraction_temp.csv")
        else:
            logger.info("Upating the retraction for release table...")
            table_schema = syn.get(RETRACTION_TABLE_ID)
            table = syn.store(Table(table_schema, new_retracted_df))
            update_version(syn, RETRACTION_TABLE_ID, comment)
        
if __name__ == "__main__":
    main()