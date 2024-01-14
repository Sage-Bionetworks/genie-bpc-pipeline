# Python
import argparse
import os
import time

import pandas as pd
import synapseclient
from synapseclient import Table
import yaml

from utils import get_production, get_adjusted, get_pressure, get_sdv, get_irr

# setup ----------------------
tic = time.time()

workdir = "."
if not os.path.exists("config.yaml"):
    workdir = "/usr/local/src/myscripts"
with open(f"{workdir}/config.yaml", "r") as stream:
    config = yaml.safe_load(stream)


parser = argparse.ArgumentParser()
parser.add_argument(
    "-s",
    "--save_synapse",
    action="store_true",
    default=False,
    help="Save updated counts on Synapse",
)
parser.add_argument(
    "-c", "--comment", type=str, help="Comment for new table snapshot version"
)
parser.add_argument(
    "-a",
    "--synapse_auth",
    type=str,
    default=None,
    help="Path to .synapseConfig file or Synapse PAT (default: normal synapse login behavior",
)
args = parser.parse_args()

save_synapse = args.save_synapse
comment = args.comment
auth = args.synapse_auth

# synapse login -------------------
syn = synapseclient.Synapse()
syn.login()


def get_current_production_record_count(synid_table_patient, cohort, phase, site=None):
    syn = synapseclient.login()

    if cohort in ["NSCLC", "CRC"] and phase == "2":
        cohort = f"{cohort}{phase}"
    if site is None:
        query = f"SELECT record_id FROM {synid_table_patient} WHERE cohort = '{cohort}'"
    else:
        query = f"SELECT record_id FROM {synid_table_patient} WHERE cohort = '{cohort}' AND redcap_data_access_group = '{site}'"

    results = syn.tableQuery(query)
    record_ids = results.asDataFrame()

    return len(record_ids)


def clear_synapse_table(table_id):
    """
    Clears a synapse table by deleting all rows.

    Parameters:
        table_id (str): The ID of the table to be cleared.

    Returns:
        int: The number of rows deleted.
    """
    syn = synapseclient.login()

    results = syn.tableQuery(f"SELECT * FROM {table_id}")
    data = results.asDataFrame()
    syn.delete(data)
    return len(data)


def update_synapse_table(table_id, data):
    """
    Updates a table in the Synapse platform with new data.

    Parameters:
        table_id (str): The ID of the table to be updated.
        data (pandas.DataFrame): The new data to be stored in the table.

    Returns:
        int: The number of rows in the updated table.
    """
    syn = synapseclient.login()

    entity = syn.get(table_id)
    project_id = entity.properties.parentId
    table_name = entity.properties.name
    cols = data.columns
    table = Table(table_name, data, parent=project_id, columns=cols)
    syn.store(table)
    return len(data)


def create_new_table_version(table_id, data, comment=""):
    """
    Create a new version of a table in Synapse.

    Parameters:
        table_id (str): The ID of the table to create a new version for.
        data (pandas.DataFrame): The data to update the table with.
        comment (str, optional): A comment to associate with the new version.

    Returns:
        int: The version number of the newly created version.
    """
    syn = synapseclient.login()
    _ = clear_synapse_table(table_id)
    _ = update_synapse_table(table_id, data)
    n_version = syn.create_snapshot_version(table_id, comment=comment)
    return n_version


# main ----------------------------
labels = [
    "cohort",
    "site",
    "phase",
    "current_cases",
    "target_cases",
    "adjusted_cases",
    "pressure",
    "sdv",
    "irr",
]
final = pd.DataFrame(columns=labels)

# gather production counts
for phase in config["phase"].keys():
    for cohort in config["phase"][phase]["cohort"].keys():
        for site in config["phase"][phase]["cohort"][cohort].get("site", {}).keys():
            n_current = get_current_production_record_count(
                synid_table_patient=config["synapse"]["bpc_patient"]["id"],
                cohort=cohort,
                phase=phase,
                site=site,
            )
            n_target = get_production(config, phase, cohort, site)
            n_adjust = get_adjusted(config, phase, cohort, site)
            n_pressure = get_pressure(config, phase, cohort, site)
            n_sdv = get_sdv(config, phase, cohort, site)
            n_irr = get_irr(config, phase, cohort, site)

            final = final.append(
                pd.Series(
                    [
                        cohort,
                        site,
                        phase,
                        n_current,
                        n_target,
                        n_adjust,
                        n_pressure,
                        n_sdv,
                        n_irr,
                    ],
                    index=labels,
                ),
                ignore_index=True,
            )

# sort
final = final.sort_values(by=["phase", "cohort", "site"])

# save ------------------------
if save_synapse:
    pass
    # n_version = create_new_table_version(table_id=config['synapse']['case_selection']['id'],
    #                                      data=final,
    #                                      comment=comment)
else:
    final.to_csv("case_selection_counts.csv", index=False)

# close out ----------------------------
if save_synapse:
    print(
        f"Table saved to Synapse as 'Case Selection Counts' ({config['synapse']['case_selection']['id']}), version {n_version}"
    )
else:
    print("Table saved locally to 'case_selection_counts.csv'")
toc = time.time()
print(f"Runtime: {round(toc - tic)} s")
