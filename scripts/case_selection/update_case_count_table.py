# Python
import argparse
import os
import time

import pandas as pd
import synapseclient
from synapseclient import Table
import yaml

from utils import get_production, get_adjusted, get_pressure, get_sdv, get_irr


def get_current_production_record_count(syn, synid_table_patient, cohort, phase, site=None):
    if cohort in ["NSCLC", "CRC"] and phase == "2":
        cohort = f"{cohort}{phase}"
    if site is None:
        query = f"SELECT record_id FROM {synid_table_patient} WHERE cohort = '{cohort}'"
    else:
        query = f"SELECT record_id FROM {synid_table_patient} WHERE cohort = '{cohort}' AND redcap_data_access_group = '{site}'"

    results = syn.tableQuery(query)
    record_ids = results.asDataFrame()

    return len(record_ids)


def clear_synapse_table(syn, table_id):
    """
    Clears a synapse table by deleting all rows.

    Parameters:
        table_id (str): The ID of the table to be cleared.

    Returns:
        int: The number of rows deleted.
    """
    results = syn.tableQuery(f"SELECT * FROM {table_id}")
    data = results.asDataFrame()
    syn.delete(data)
    return len(data)


def update_synapse_table(syn, table_id, data):
    """
    Updates a table in the Synapse platform with new data.

    Parameters:
        table_id (str): The ID of the table to be updated.
        data (pandas.DataFrame): The new data to be stored in the table.

    Returns:
        int: The number of rows in the updated table.
    """
    entity = syn.get(table_id)
    project_id = entity.properties.parentId
    table_name = entity.properties.name
    cols = data.columns
    table = Table(table_name, data, parent=project_id, columns=cols)
    syn.store(table)
    return len(data)


def create_new_table_version(syn, table_id, data, comment=""):
    """
    Create a new version of a table in Synapse.

    Parameters:
        table_id (str): The ID of the table to create a new version for.
        data (pandas.DataFrame): The data to update the table with.
        comment (str, optional): A comment to associate with the new version.

    Returns:
        int: The version number of the newly created version.
    """
    _ = clear_synapse_table(syn, table_id)
    _ = update_synapse_table(syn, table_id, data)
    n_version = syn.create_snapshot_version(table_id, comment=comment)
    return n_version


def main():
    """
    The main function that performs the core logic of the program.
    It parses command line arguments, retrieves configuration settings,
    gathers production counts for each phase, cohort, and site,
    sorts the final results, and saves the data either locally or on Synapse.
    """
    tic = time.time()
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
    args = parser.parse_args()

    save_synapse = args.save_synapse
    comment = args.comment

    syn = synapseclient.login()

    workdir = "."
    if not os.path.exists("config.yaml"):
        workdir = "/usr/local/src/myscripts"
    with open(f"{workdir}/config.yaml", "r") as stream:
        config = yaml.safe_load(stream)

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
    case_count_table_df = pd.DataFrame(columns=labels)

    # gather production counts
    for phase in config["phase"].keys():
        for cohort in config["phase"][phase]["cohort"].keys():
            for site in config["phase"][phase]["cohort"][cohort].get("site", {}).keys():
                n_current = get_current_production_record_count(
                    syn=syn,
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
                new_row = pd.DataFrame(
                    [
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
                        ]
                    ],
                    columns=labels,
                )

                case_count_table_df = pd.concat([case_count_table_df, new_row], ignore_index=True)

    # sort
    case_count_table_df = case_count_table_df.sort_values(
        by=["phase", "cohort", "site"]
    )

    # save ------------------------
    if save_synapse:
        n_version = create_new_table_version(
            syn=syn,
            table_id=config["synapse"]["case_selection"]["id"],
            data=case_count_table_df,
            comment=comment,
        )
    else:
        case_count_table_df.to_csv("case_selection_counts.csv", index=False)

    # close out ----------------------------
    if save_synapse:
        print(
            f"Table saved to Synapse as 'Case Selection Counts' ({config['synapse']['case_selection']['id']}), version {n_version}"
        )
    else:
        print("Table saved locally to 'case_selection_counts.csv'")
    toc = time.time()
    print(f"Runtime: {round(toc - tic)} s")


if __name__ == "__main__":
    main()
