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
    phases = [
        name for name in config["phase"].keys() if not name.endswith("_additional")
    ]
    return sorted(phases)


def get_cohorts(config, phase):
    cohorts = list(config["phase"][phase]["cohort"].keys())
    return sorted(cohorts)


def get_sites(config, phase, cohort):
    sites = list(config["phase"][phase]["cohort"][cohort]["site"].keys())
    return sorted(sites)


def get_min_seq_date(config, phase, cohort, site):
    if "date" in config["phase"][phase]["cohort"][cohort]["site"][site]:
        return config["phase"][phase]["cohort"][cohort]["site"][site]["date"]["seq_min"]
    return config["phase"][phase]["cohort"][cohort]["date"]["seq_min"]


def get_max_seq_date(config, phase, cohort, site):
    if "date" in config["phase"][phase]["cohort"][cohort]["site"][site]:
        return config["phase"][phase]["cohort"][cohort]["site"][site]["date"]["seq_max"]
    return config["phase"][phase]["cohort"][cohort]["date"]["seq_max"]


def get_oncotree_codes(config, phase, cohort, delim=";"):
    oncotree_vec = config["phase"][phase]["cohort"][cohort]["oncotree"]["allowed_codes"]
    oncotree_str = delim.join(oncotree_vec)
    return oncotree_str


def get_target_count(config, phase, cohort, site):
    target = config["phase"][phase]["cohort"][cohort]["site"][site]["production"]
    return int(target)


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
