# Description: Create a Synapse table with GENIE BPC
#   case selection criteria.
# Author: Haley Hunter-Zinck
# Date: 2022-05-25

# setup ----------------------------

import time
import os
import argparse
import pandas as pd
import yaml
import synapseclient
from synapseclient import Table

from utils import create_synapse_table_version

tic = time.time()

# user input ----------------------------

parser = argparse.ArgumentParser()
parser.add_argument(
    "-s",
    "--save_synapse",
    action="store_true",
    default=False,
    help="Save updated counts on Synapse (default: FALSE)",
)
parser.add_argument(
    "-c",
    "--comment",
    type=str,
    default="update to case selection criteria",
    help="Comment for new table snapshot version (default: 'update to case selection criteria')",
)
parser.add_argument(
    "-v",
    "--verbose",
    action="store_true",
    default=False,
    help="Output script messages to the user (default: FALSE)",
)
args = parser.parse_args()

save_synapse = args.save_synapse
comment = args.comment
verbose = args.verbose

# parameters
file_config = "config.yaml"
file_output = "bpc_case_selection_criteria.csv"


# functions ----------------------------
def get_phases(config):
    """
    Return a sorted list of phases based on the given config.

    Parameters:
    - config (dict): A dictionary containing the configuration of the phases.

    Returns:
    - list: A sorted list of phases extracted from the given config.
    """
    phases = [
        name for name in config["phase"].keys() if not name.endswith("_additional")
    ]
    return sorted(phases)


def get_cohorts(config, phase):
    """
    Retrieve the list of cohorts for a given phase.

    Parameters:
        config (dict): The configuration dictionary.
        phase (str): The phase for which to retrieve the cohorts.

    Returns:
        list: The sorted list of cohorts for the given phase.
    """
    cohorts = list(config["phase"][phase]["cohort"].keys())
    return sorted(cohorts)


def get_sites(config, phase, cohort):
    """
    Get the sites for a given configuration, phase, and cohort.

    Args:
        config (dict): The configuration dictionary.
        phase (str): The phase to retrieve sites from.
        cohort (str): The cohort to retrieve sites from.

    Returns:
        list: A sorted list of site names.
    """
    sites = list(config["phase"][phase]["cohort"][cohort]["site"].keys())
    return sorted(sites)


def get_min_seq_date(config, phase, cohort, site):
    """
    Retrieves the minimum sequential date from the given configuration based on the specified phase, cohort, and site.

    Parameters:
        config (dict): The configuration dictionary.
        phase (str): The phase to retrieve the sequential date from.
        cohort (str): The cohort to retrieve the sequential date from.
        site (str): The site to retrieve the sequential date from.

    Returns:
        str: The minimum sequential date from the configuration.
    """
    if "date" in config["phase"][phase]["cohort"][cohort]["site"][site]:
        return config["phase"][phase]["cohort"][cohort]["site"][site]["date"]["seq_min"]
    return config["phase"][phase]["cohort"][cohort]["date"]["seq_min"]


def get_max_seq_date(config, phase, cohort, site):
    """
    Retrieves the maximum sequence date based on the given configuration, phase, cohort, and site.

    Parameters:
    - config (dict): The configuration dictionary.
    - phase (str): The phase value.
    - cohort (str): The cohort value.
    - site (str): The site value.

    Returns:
    - str: The maximum sequence date.

    Raises:
    - KeyError: If any of the specified keys are not found in the configuration dictionary.
    """
    if "date" in config["phase"][phase]["cohort"][cohort]["site"][site]:
        return config["phase"][phase]["cohort"][cohort]["site"][site]["date"]["seq_max"]
    return config["phase"][phase]["cohort"][cohort]["date"]["seq_max"]


def get_oncotree_codes(config, phase, cohort, delim=";"):
    """
    Generate the oncotree codes for a specific phase and cohort.

    Parameters:
        - config (dict): The configuration dictionary containing the phase, cohort, and oncotree information.
        - phase (str): The name of the phase.
        - cohort (str): The name of the cohort.
        - delim (str, optional): The delimiter used to join the oncotree codes. Defaults to ";".

    Returns:
        - oncotree_str (str): A string containing the oncotree codes joined by the specified delimiter.
    """
    oncotree_vec = config["phase"][phase]["cohort"][cohort]["oncotree"]["allowed_codes"]
    oncotree_str = delim.join(oncotree_vec)
    return oncotree_str


def get_target_count(config, phase, cohort, site):
    """
    Retrieves the target count for a specific phase, cohort, and site from the given configuration.

    Parameters:
        config (dict): A dictionary representing the configuration.
        phase (str): The phase for which to retrieve the target count.
        cohort (str): The cohort for which to retrieve the target count.
        site (str): The site for which to retrieve the target count.

    Returns:
        int: The target count for the specified phase, cohort, and site.
    """
    target = config["phase"][phase]["cohort"][cohort]["site"][site]["production"]
    return int(target)


def main():
    """
    Main function that performs the following tasks:

    1. Logs into the synapse client.
    2. Reads the configuration file.
    3. Gathers case selection criteria from the configuration file.
    4. Retrieves phases, cohorts, sites, min_seq_date, max_seq_date, oncotree, and target count.
    5. Creates a DataFrame to store the collected data.
    6. Updates the case selection criteria table in Synapse if save_synapse is True.
    7. Writes the case selection criteria to a local file if save_synapse is False.
    8. Prints the runtime of the function.
    """
    syn = synapseclient.login()

    # read ----------------------------
    # configuration file
    with open(file_config, "r") as stream:
        config = yaml.safe_load(stream)

    # main ----------------------------
    # storage
    labels = [
        "phase",
        "cohort",
        "site",
        "min_seq_date",
        "max_seq_date",
        "oncotree",
        "target",
    ]
    tab = pd.DataFrame(columns=labels)

    if verbose:
        print("Gathering case selection criteria from configuration file...")

    phases = get_phases(config)
    for phase in phases:
        cohorts = get_cohorts(config, phase)
        for cohort in cohorts:
            sites = get_sites(config, phase, cohort)
            for site in sites:
                min_seq_date = get_min_seq_date(config, phase, cohort, site)
                max_seq_date = get_max_seq_date(config, phase, cohort, site)
                oncotree = get_oncotree_codes(config, phase, cohort)
                target = get_target_count(config, phase, cohort, site)

                row = [phase, cohort, site, min_seq_date, max_seq_date, oncotree, target]
                tab = tab.append(pd.Series(row, index=labels), ignore_index=True)

    if save_synapse:
        if verbose:
            print(
                f"Update case selection criteria table ({config['synapse']['selection_criteria']['id']})..."
            )
        n_version = create_synapse_table_version(
            syn=syn,
            table_id=config["synapse"]["selection_criteria"]["id"],
            data=tab,
            comment=comment,
            append=False,
        )
        if verbose:
            print(f"Updated to version {n_version}...")
    else:
        if verbose:
            print(f"Writing case selection criteria to local file ({file_output})...")
        tab.to_csv(file_output, index=False)

    # close out ----------------------------
    toc = time.time()
    print(f"Runtime: {round(toc - tic)} s")


if __name__ == "__main__":
    main()
