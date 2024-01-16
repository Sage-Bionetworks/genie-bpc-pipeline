# Python
import argparse
import os
import time

import pandas as pd
import synapseclient
from synapseclient import Table
import yaml

from utils import get_production, get_adjusted, get_pressure, get_sdv, get_irr, create_synapse_table_version


def get_current_production_record_count(
    syn, synid_table_patient, cohort, phase, site=None
):
    """
    Returns the count of current production records for a given cohort and phase.

    Parameters:
        syn (Synapse): The Synapse client object.
        synid_table_patient (str): The Synapse table ID for the patient records.
        cohort (str): The cohort name.
        phase (str): The phase of the cohort.
        site (str, optional): The site name. Defaults to None.

    Returns:
        int: The count of current production records.
    """
    if cohort in ["NSCLC", "CRC"] and phase == "2":
        cohort = f"{cohort}{phase}"
    if site is None:
        query = f"SELECT record_id FROM {synid_table_patient} WHERE cohort = '{cohort}'"
    else:
        query = f"SELECT record_id FROM {synid_table_patient} WHERE cohort = '{cohort}' AND redcap_data_access_group = '{site}'"

    results = syn.tableQuery(query)
    record_ids = results.asDataFrame()

    return len(record_ids)


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

                case_count_table_df = pd.concat(
                    [case_count_table_df, new_row], ignore_index=True
                )

    # sort
    case_count_table_df = case_count_table_df.sort_values(
        by=["phase", "cohort", "site"]
    )

    # save ------------------------
    if save_synapse:
        n_version = create_synapse_table_version(
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
