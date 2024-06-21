# !/usr/bin/python
'''
Example:
python update_data_element_catalog.py -v v3.1.1
'''
import argparse
import re
import pandas
import numpy

from utilities import *

CATALOG_ID = "syn21431364"
SOR_ID = "syn22294851"

def set_up(args):
    """Set up with the genereal arguments

    Args:
        args: argument from input

    Returns:
        list: dry run flag, synapse login, and logger
    """
    dry_run = args.dry_run

    #login to synapse
    syn = synapse_login(args.synapse_config)
    
    #create logger
    logger_name = "testing" if dry_run else "production"
    logger = setup_custom_logger(logger_name)
    logger.info('Updating BPC data element catalog!')
    return dry_run, syn, logger

def _get_dd_info(syn, version):
    """
    Get the non-PHI data dictionary Synapse ID and cohort by version number
    """
    prissmm_info = syn.tableQuery("SELECT id, name, cohort FROM syn22684834 WHERE name=\'%s\'" % version).asDataFrame()
    #TODO: error message if the version does not exist
    for file_info in syn.getChildren(prissmm_info['id'][0]):
        if file_info['name'] == "Data Dictionary non-PHI":
            syn_id = file_info['id']
    return syn_id, prissmm_info['cohort'][0]

def _get_choices_info(choices):
    """
    Get the number of choices and max length of choices for given choices
    """
    choices_list = choices.split('|')
    choices_keys, choices_list = map(list, zip(*(s.split(',',1) for s in choices_list)))
    choices_keys = [s.strip() for s in choices_keys]
    choices_keys = ",".join(choices_keys)
    choices_list = [s.strip() for s in choices_list]
    max_len = len(max(choices_list, key=len))
    # round up the max length to the nearest 10
    max_len = round((max_len+4)/10)*10
    return pandas.Series([len(choices_list),max_len,choices_keys])

def _get_syn_col_type(var_type, validation):
    """
    Get Synapse Table column type by variable type and validation
    """
    if validation == "integer":
        return "INTEGER"
    elif validation == "numeric":
        return "DOUBLE"
    elif var_type == "notes":
        return "LARGETEXT"
    else:
        return "STRING"

def _create_new_row(df, cohort):
    """
    Create new row for updating the data element catalog
    """
    # add new and update old variables to data element catalog on Synapse
    # add: variable, instrument, dataType='curated', type, label, cohort_dd, synColType, synColSize, numCols, colLabels
    df.to_csv("test.csv", index=False)
    df['synColType'] = df.apply(lambda row: _get_syn_col_type(row.type, row.validation), axis=1)
    df_choices = df[df['type'].isin(['dropdown','radio','checkbox'])]
    df_choices.loc[:, ['choices_num','max_len','choices_key']] = df_choices['choices'].apply(_get_choices_info)
    non_checkbox_index = df_choices.index[df_choices.type.isin(['dropdown','radio'])]
    checkbox_index = df_choices.index[df_choices.type == "checkbox"]
    df.loc[non_checkbox_index,'synColSize'] = df_choices.loc[non_checkbox_index,'max_len']
    if len(checkbox_index) > 0:
        df.loc[checkbox_index, ['synColSize','numCols','colLabels']] = df_choices.loc[non_checkbox_index,['max_len','choices_num','choices_key']]
    df.loc[df.type=="yesno", 'synColSize'] = 20
    df['dataType'] = 'curated'
    df[cohort+'_dd'] = True
    df.drop(columns=['choices','validation'],axis=1,inplace=True)
    return df

def _update_by_data_dictionary(data_dictionary, data_element_catalog, logger):
    """
    Compare data dictionary and data element catalog
    """
    # check for the variables that need to be added and removed
    dd_vars = data_dictionary['variable']
    dec_vars = data_element_catalog['variable']
    vars_to_add = list(set(dd_vars)-set(dec_vars))
    vars_to_add_df = data_dictionary[data_dictionary['variable'].isin(vars_to_add)]
    vars_to_rm = list(set(dec_vars)-set(dd_vars))
    vars_to_rm_df = data_element_catalog[data_element_catalog['variable'].isin(vars_to_rm)]
    logger.info("Number of new variables: %s \n" % len(vars_to_add)+'\n'.join(vars_to_add))
    logger.info("Number of removed variables: %s \n" % len(vars_to_rm)+'\n'.join(vars_to_rm))
    # check for variable wtih choices to update 
    vars_with_choices = data_dictionary[data_dictionary['type'].isin(['dropdown','radio','checkbox'])]
    vars_with_choices = vars_with_choices.merge(data_element_catalog, on="variable")
    vars_with_choices.index = vars_with_choices['index']
    vars_with_choices[['choices_num','max_len','choices_key']] = vars_with_choices['choices'].apply(_get_choices_info)
    # check for variables with choices that the max_len(choices) > synColSize
    # TODO: yesno can be changed to choices 
    vars_to_update = vars_with_choices.query('max_len > synColSize')
    vars_to_update['synColSize'] = vars_to_update['max_len']
    # check for number of checkbox variables > numCols
    vars_checkbox = vars_with_choices[vars_with_choices['type']=='checkbox']
    vars_checkbox_update = vars_checkbox.query('choices_num > numCols')
    vars_checkbox_update['numCols'] = vars_checkbox_update['choices_num']
    vars_checkbox_update['colLabels'] = vars_checkbox_update['choices_key']
    # combined variables for update
    index_to_update = vars_to_update.index[vars_to_update['variable'].isin(vars_checkbox_update['variable'])]
    vars_to_update.loc[index_to_update,'numCols'] = vars_checkbox_update.loc[index_to_update,'numCols']
    vars_to_update.loc[index_to_update,'colLabels'] = vars_checkbox_update.loc[index_to_update,'colLabels']
    vars_checkbox_update.drop(index=index_to_update,inplace=True)
    vars_to_update_df = pandas.concat([vars_to_update, vars_checkbox_update])

    logger.info("Number of updated variables: %s" % vars_to_update_df.shape[0])
    logger.info("Updated Synapse column size: %s \n" % vars_to_update.shape[0]+'\n'.join(vars_to_update['variable']))
    logger.info("Updated Synapse column number only: %s \n" % vars_checkbox_update.shape[0]+'\n'.join(vars_checkbox_update['variable']))
    logger.info("Updated both columns size and number: %s \n" % +len(index_to_update)+'\n'.join(vars_to_update.loc[index_to_update, 'variable']))
    return vars_to_add_df, vars_to_rm_df, vars_to_update_df

#TODO: 
# combine the add/update/remove into one syn.store
# determine the procedure for variables of removal
def update_by_data_dictionary(args):
    dry_run, syn, logger = set_up(args)
    dd_syn_id, cohort = _get_dd_info(syn, args.version)
    data_dictionary = pandas.read_csv(syn.get(dd_syn_id).path,
                                      usecols=[0,1,3,4,5,7],
                                      header=0, 
                                      names=["variable","instrument","type","label","choices","validation"]
                                      )
    curated_var_catalog = syn.tableQuery("SELECT variable, synColSize, numCols \
        FROM %s WHERE dataType='curated'" % CATALOG_ID).asDataFrame()
    curated_var_catalog.index = curated_var_catalog.index.map(str)
    curated_var_catalog['index'] = curated_var_catalog.index
    vars_to_add_df, vars_to_rm_df, vars_to_update_df = \
        _update_by_data_dictionary(data_dictionary, curated_var_catalog, logger)
    # add new and update old variables to data element catalog on Synapse
        # add: variable, instrument, dataType='curated', type, label, cohort-dd, synColType, synColSize, numCols
        # update: variable, synColSize, numCols
    if not dry_run:
        if not vars_to_update_df.empty: 
            vars_to_update_df = vars_to_update_df[['synColSize','numCols','colLabels']]
            vars_to_update_df = syn.store(Table(CATALOG_ID, vars_to_update_df))
        if not vars_to_add_df.empty: 
            vars_to_add_df = _create_new_row(vars_to_add_df, cohort)
            vars_to_add_df = syn.store(Table(CATALOG_ID, vars_to_add_df))

def download_bpc_sor(syn, logger):
    """Download the BPC Scope of Release

    Args:
        syn (Object): Synapse Credential
        logger (Object): logger for tracking

    Returns:
        pandas.DataFrame: Scope of Release
    """
    logger.info("Downloading BPC Scope of Release...")
    sor = pandas.read_excel(syn.get(SOR_ID).path, sheet_name="Data Dictionary")
    # get the list of columns we need
    sor.columns = sor.columns.str.lower()
    sor = sor.filter(regex='^varname|^type|dataset|display name|shared|cbio')
    # rename the columns
    sor.drop(columns=['cbio varname'],inplace=True)
    sor.rename(columns={'varname': 'variable', 
                        'type': 'dataType',
                        'display name': 'label'}, 
               inplace=True)
    sor.loc[:, ~sor.columns.isin(['dataset','label'])] = \
        sor.loc[:, ~sor.columns.isin(['dataset','label'])].apply(lambda x: x.str.lower())
    sor.dataType.replace(['project genie tier 1 data', 'tumor registry'],'curated',inplace=True)
    sor.variable = sor.variable.str.strip()
    return sor

def _select_columns_by_release_info(col_list, cohort, release_version, logger):
    """Helper function to find the columns with given release information

    Args:
        col_list (list): colnames from sor
        cohort (str): given cohort
        release_version (str): given release version
        logger (Object): logger for tracking

    Raises:
        Exception: need to check the sor file or release information table if 2 columns
                   are not found per cohort-release_version

    Returns:
        list: clinical release column name, cbio release column name
    """
    if str(release_version) == "1.1":
        release_version = "1"
    r = re.compile(".*%s.*%s.*" % (cohort.lower(),str(release_version)))
    related_columns = list(filter(r.match, col_list))
    if len(related_columns) != 2:
        logger.error("Cannot find 2 columns in sor file for %s v%s" %(cohort, release_version))
        raise Exception("Please check the scope of release file. Cannot find the 2 columns.")
    for col in related_columns:
        if "cbio" in col:
            cbio_col_name = col
        else:
            clinical_col_name = col
    return [clinical_col_name, cbio_col_name]

def _get_release_type_by_info(clinical_release, cbio_release, release_type):
    """Helper function to get release type in Data Element Catalog

    Args:
        clinical_release (str): clinical releases status of the variable
        cbio_release (str): cbio release status of the variable
        release_type (str): release type defined in Release Info Table

    Returns:
        str: release type in Data Element Catalog
    """
    yes_value_list = ["yes","always","index cancer only","non-index cancer only"]
    dec_release_type = "private"
    if release_type == "consortium":
        release_type = "project"
    if clinical_release in yes_value_list:
        dec_release_type = release_type
    elif cbio_release in yes_value_list:
        dec_release_type = "consortium"
    return dec_release_type

def _process_release_type(var_row, release_info):
    """Helper function to process the release type per row

    Args:
        var_row (pandas.Series): Row of the variable
        release_info (pandas.DataFrame): release information

    Returns:
        pandas.Series: a series for variable with release type columns added
    """
    temp_dict = {}
    for _, cohort_row in release_info.iterrows():
        dec_release_type = _get_release_type_by_info(var_row[cohort_row['clinical_col_name']],
                                                     var_row[cohort_row['cbio_col_name']],
                                                     cohort_row['release_type'])
        temp_dict[cohort_row['cohort']+"_sor"] = dec_release_type
    return pandas.Series(temp_dict)

def format_bpc_sor(sor, release_info, logger):
    """Format Scope of Release with DEC release type columns

    Args:
        sor (pandas.DataFrame): Scope of release
        release_info (pandas.DataFrame): Release information
        logger (Object): logger for tracking

    Returns:
        pandas.DataFrame: Scope of Release with expected DEC release type
    """
    # Format sor by removing dup variables
    sor['variable'] = sor['variable'].apply(lambda x: re.sub(r"""___\d+""","", x))
    sor.drop_duplicates(['variable','dataset'],inplace=True)
    # Remove Synapse Tables variables
    r = re.compile("synapse_*")
    syn_table_vars = list(filter(r.match, sor.variable))
    sor.drop(sor.loc[sor['variable'].isin(syn_table_vars)].index, inplace=True)
    logger.info('Removing Synapse Table variables...')
    # Add release status columns to release info
    release_info[['clinical_col_name', 'cbio_col_name']] = \
        release_info.apply(lambda x: _select_columns_by_release_info(list(sor.columns),
                                                                     x['cohort'],
                                                                     x['release_version'],
                                                                     logger),
                           result_type='expand', axis=1)
    # Add release scope columns to sor
    sor_formatted = sor.join(sor.apply(lambda x: _process_release_type(x, release_info), 
                                       axis="columns"))
    return sor_formatted

def _update_by_release_scope(sor_formatted, data_element_catalog, logger):
    sor_derived_vars = set(sor_formatted[sor_formatted['dataType']=="derived"]['variable'])
    if 'redacted' in sor_derived_vars:
        sor_derived_vars.remove('redacted')
    dec_derived_vars = set(data_element_catalog[data_element_catalog['dataType']=="derived"]['variable'])
    r = re.compile(".*_sor")
    sor_columns = list(filter(r.match, sor_formatted.columns))
    # variables to add
    vars_to_add = list(sor_derived_vars-dec_derived_vars)
    vars_to_add_df = sor_formatted[sor_formatted['variable'].isin(vars_to_add)]
    vars_to_add_df_col = ['variable','dataType','dataset','label']+sor_columns
    vars_to_add_df = vars_to_add_df[vars_to_add_df_col]
    # variables to remove
    vars_to_rm = list(dec_derived_vars-sor_derived_vars)
    logger.info("Number of new derived variables: %s \n" % len(vars_to_add)+'\n'.join(vars_to_add))
    logger.info("Number of removed derived variables: %s \n" % len(vars_to_rm)+'\n'.join(vars_to_rm))
    # TODO: variables to update; waiting for stats team
    vars_to_update_df = ""
    return vars_to_add_df, vars_to_rm, vars_to_update_df
    
def update_by_release_scope(args):
    dry_run, syn, logger = set_up(args)
    sor = download_bpc_sor(syn, logger)
    release_info = syn.tableQuery("SELECT cohort, release_version, release_type \
                                   FROM syn27628075 \
                                   WHERE current is true").asDataFrame()
    sor_formatted = format_bpc_sor(sor, release_info, logger)
    data_element_catalog_query = syn.tableQuery("SELECT * FROM %s" % CATALOG_ID)
    data_element_catalog = data_element_catalog_query.asDataFrame()
    vars_to_add_df, vars_to_rm_df, vars_to_update_df = \
        _update_by_release_scope(sor_formatted, data_element_catalog, logger)
    if not dry_run:
        table_schema = syn.get(CATALOG_ID)
        if not vars_to_update_df.empty:
            syn.store(Table(table_schema, vars_to_update_df, 
                            etag=data_element_catalog_query.etag))
        if not vars_to_add_df.empty:
            logger.info("Adding new variables to the data element catalog...")
            syn.store(Table(table_schema,vars_to_add_df))

def main():
    #add arguments
    parser = argparse.ArgumentParser(
        description='Update BPC data element catalog')
    subparsers = parser.add_subparsers()
    # Create a dd subcommand    
    parser_dd = subparsers.add_parser('dd', help='update by data dictionary')
    parser_dd.add_argument(
        "-v", "--version",
        help="Version of the data dictionary, i.e. v3.1.1"
    )
    parser_dd.set_defaults(func=update_by_data_dictionary)
    # Create a sor subcommand       
    parser_sor = subparsers.add_parser('sor', help='update by scope of release')
    parser_sor.set_defaults(func=update_by_release_scope)
    # general commands
    parser.add_argument(
        "-c", "--synapse_config",
        default=synapseclient.client.CONFIG_FILE,
        help="Synapse credentials file"
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="dry run flag"
    )
    
    if len(sys.argv) <= 1:
        sys.argv.append('--help')
    
    args = parser.parse_args()
    if args.func:
        args.func(args)

if __name__ == "__main__":
    main()