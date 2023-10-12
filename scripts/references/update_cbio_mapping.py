import argparse
from datetime import datetime
import json

import pandas as pd
import synapseclient
from synapseclient import Table

# Setup
tic = datetime.timestamp(datetime.now())

# Command-line argument parsing
parser = argparse.ArgumentParser(description="Update the cbio mapping Synapse Table using the CSV file provided by cBioPortal.")
parser.add_argument("-s", "--save_to_synapse", action="store_true", default=False, help="Save mapping to Synapse table and delete local output file")
parser.add_argument("-c", "--comment", type=str, default=None, help="Comment for table snapshot if saving to Synapse (optional)")
parser.add_argument("-a", "--auth", type=str, default=None, help="Synapse personal access token or path to .synapseConfig (default: normal synapse login behavior)")
parser.add_argument("-v", "--verbose", action="store_true", default=False, help="Print script progress to the user")
args = parser.parse_args()
save_to_synapse = args.save_to_synapse
comment = args.comment
auth = args.auth
verbose = args.verbose

if verbose:
    print("Parameters:")
    print(f"- save on Synapse: {save_to_synapse}")
    print(f"- comment: '{'auto-generated' if comment is None else comment}'")
    print(f"- verbose: {verbose}")

# Parameters
file_id = "syn25585554"
tbl_id = "syn25712693"
cohorts = ["NSCLC", "CRC", "BrCa", "PANC", "Prostate", "BLADDER"]
outfile = "cbio_mapping_table_update.csv"

# Functions
def get_auth_token(path):
    with open(path, 'r') as config_file:
        config_data = json.load(config_file)
        return config_data.get('authtoken', None)

def now(time_only=False, tz="US/Pacific"):
    if time_only:
        return datetime.now().strftime("%H:%M:%S")
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Synapse login
syn = synapseclient.Synapse()

if auth:
    if auth == "~/.synapseConfig" or auth is None:
        syn.login(silent=True)
    else:
        token = get_auth_token(auth)
        if token is not None:
            syn.login(authToken=token, silent=True)
        else:
            print("Failed to retrieve the authentication token from the specified config file.")
            exit(1)
else:
    syn.login(silent=True)

# Read
if verbose:
    print(f"{now(time_only=True)}: reading mapping file CSV...")

entity = syn.get(file_id)
mapping_file = pd.read_csv(entity.path)
table = syn.tableQuery(f"select * from {tbl_id}")
columns = table.headers
col_names = [column.name for column in columns]

# Mapping
if comment is None:
    utc_mod = entity.modifiedOn
    pt_mod = utc_mod.replace(tzinfo=synapseclient.utils.from_tz_string("America/Los_Angeles"))
    comment = f"mapping file update from {pt_mod.strftime('%Y-%m-%d')} PT ({file_id}.{entity.versionNumber})"

if verbose:
    print(f"{now(time_only=True)}: formatting mapping information...")

# Make adjustments to match the table schema
mapping_file = mapping_file.rename(columns={"BRCA": "BrCa", "PROSTATE": "Prostate", "inclusion.criteria": "inclusion_criteria"})
for cohort in cohorts:
    mapping_file[cohort] = mapping_file[cohort].replace({"Y": True, "N": False, "TBD": False})
mapping_file['data_type'] = mapping_file['data_type'].str.lower()
mapping_file['data_type'] = mapping_file['data_type'].replace({"tumor_registry": "curated"})

# Write
if save_to_synapse:
    if verbose:
        print(f"{now(time_only=True)}: updating Synapse table '{syn.get(tbl_id).name}' ({tbl_id}) with snapshot...")

    deleted = syn.tableQuery(f"select * from {tbl_id}")
    syn.delete(deleted)
    syn.store(Table(tbl_id, mapping_file))

    snapshot_comment = f"{{'snapshotComment': '{comment}'}}"
    snapshot_comment = snapshot_comment.replace("'", '"')
    syn.restPOST(f"/entity/{tbl_id}/table/snapshot", body=snapshot_comment)

    snapshot_version = syn.get(tbl_id).versionNumber
    if verbose:
        print(f"{now(time_only=True)}: updated table {syn.get(tbl_id).name} ({tbl_id}) to version {snapshot_version} with comment '{comment}'")
else:
    mapping_file.to_csv(outfile, index=False)

    if verbose:
        print(f"{now(time_only=True)}: table update written to '{outfile}'")

# Close out
toc = datetime.timestamp(datetime.now())

if verbose:
    print(f"Runtime: {round(toc - tic)} s")
