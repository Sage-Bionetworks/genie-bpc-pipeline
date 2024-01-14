# Description: Workflow for conducting BPC case selection and constructing both
#   output files representing eligibility criteria, ID lists, and a reports.
# Author: Haley Hunter-Zinck
# Date: 2021-09-22

# pre-setup --------------------------------

import argparse
import yaml
import os
import subprocess
import time
from synapseclient import File, Activity

# Assuming shared_fxns.R is converted to shared_fxns.py
from utils import *
import perform_case_selection

# parameters
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# user input ----------------------------

parser = argparse.ArgumentParser()
parser.add_argument("-p", "--phase", help="BPC phase")
parser.add_argument("-c", "--cohort", help="BPC cohort")
parser.add_argument("-s", "--site", help="BPC site")
parser.add_argument(
    "-u",
    "--save_synapse",
    action="store_true",
    default=False,
    help="Save output to Synapse",
)
args = parser.parse_args()

phase = args.phase
cohort = args.cohort
site = args.site
save_synapse = args.save_synapse

# check user input -----------------

phase_str = ", ".join(config["phase"].keys())
assert (
    phase in config["phase"]
), f"Error: phase {phase} is not valid.  Valid values: {phase_str}"

cohort_in_config = config["phase"][phase]["cohort"].keys()
cohort_str = ", ".join(cohort_in_config)
assert (
    cohort in cohort_in_config
), f"Error: cohort {cohort} is not valid for phase {phase}.  Valid values: {cohort_str}"

sites_in_config = get_sites_in_config(
    config, phase, cohort
)  # Assuming get_sites_in_config is a function in shared_fxns
site_str = ", ".join(sites_in_config)
assert (
    site in sites_in_config
), f"Error: site {site} is not valid for phase {phase} and cohort {cohort}.  Valid values: {site_str}"


tic = time.time()

syn = synapseclient.login()

# file names
file_report = f"{cohort}_{site}_phase{phase}_case_selection.html".lower()
file_matrix = f"{cohort}_{site}_phase{phase}_eligibility_matrix.csv".lower()
file_selection = f"{cohort}_{site}_phase{phase}_case_selection.csv".lower()
file_add = f"{cohort}_{site}_phase{phase}_samples.csv".lower()

# synapse
synid_folder_output = get_folder_synid_from_path(
    synid_folder_root=config["synapse"]["ids"]["id"], path=f"{cohort}/{site}"
)

# provenance exec
prov_exec_selection = "https://github.com/Sage-Bionetworks/genie-bpc-pipeline/tree/develop/scripts/case_selection/perform_case_selection.R"
prov_exec_add = prov_exec_selection
prov_exec_report = "https://github.com/Sage-Bionetworks/genie-bpc-pipeline/tree/develop/scripts/case_selection/perform_case_selection.Rmd"

# provancne used
prov_used_selection = [
    config["synapse"]["main_patient"]["id"],
    config["synapse"]["main_sample"]["id"],
    config["synapse"]["bpc_patient"]["id"],
]
prov_used_add = prov_used_selection + [config["synapse"]["bpc_sample"]["id"]]
prov_used_report = ""

# additional parameters
flag_additional = "addition" in phase


def save_to_synapse(
    path,
    parent_id,
    file_name=None,
    prov_name=None,
    prov_desc=None,
    prov_used=None,
    prov_exec=None,
):
    if file_name is None:
        file_name = path
    file = File(path=path, parentId=parent_id, name=file_name)
    if prov_name or prov_desc or prov_used or prov_exec:
        act = Activity(
            name=prov_name, description=prov_desc, used=prov_used, executed=prov_exec
        )
        file = syn.store(file, activity=act)
    else:
        file = syn.store(file)
    return True


# case selection ----------------------------

# construct eligibility matrices + case lists
# os.system(f"Rscript perform_case_selection.R -p {phase} -c {cohort} -s {site}")
perform_case_selection.main(config=config, phase=phase, cohort=cohort, site=site)

if not flag_additional:
    # render eligibility report
    # quarto render case_selection.qmd -P phase:1 -P cohort:NSCLC -P site:DFCI
    quarto_render_cmd = ['quarto', 'render', 'perform_case_selection.ipynb', f'-P phase:{phase}', f'-P cohort:{cohort}', f'-P site:{site}']
    print(" ".join(quarto_render_cmd))
    subprocess.run(quarto_render_cmd)

# load to synapse --------------------

# store case selection files
if save_synapse:
    if os.path.exists(file_selection):
        save_to_synapse(
            path=file_matrix,
            parent_id=synid_folder_output,
            prov_name="Eligibility matrix",
            prov_desc="Reports eligibility criteria and values for all possible patients",
            prov_used=prov_used_selection,
            prov_exec=prov_exec_selection,
        )
        save_to_synapse(
            path=file_selection,
            parent_id=synid_folder_output,
            prov_name="Eligible cohort",
            prov_desc="Cohort of eligible patient IDs",
            prov_used=prov_used_selection,
            prov_exec=prov_exec_selection,
        )

        prov_used_report = get_file_synid_from_path(
            synid_folder_root=config["synapse"]["ids"]["id"],
            path=f"{cohort}/{site}/{file_matrix}",
        )
        save_to_synapse(
            path=file_report,
            parent_id=synid_folder_output,
            prov_name="Summary of eligibility",
            prov_desc="Summary of steps and information for selection eligible patients",
            prov_used=prov_used_report,
            prov_exec=prov_exec_report,
        )

        # local clean-up
        os.remove(file_matrix)
        os.remove(file_selection)
        os.remove(file_report)
    elif os.path.exists(file_add):
        save_to_synapse(
            path=file_add,
            parent_id=synid_folder_output,
            prov_name="Summary of eligibility",
            prov_desc="Summary of steps and information for selection eligible patients",
            prov_used=prov_used_add,
            prov_exec=prov_exec_add,
        )

        # local clean-up
        os.remove(file_add)

# close out ----------------------------

if save_synapse:
    print(f"Output saved to Synapse ({synid_folder_output})")

toc = time.time()
print(f"Runtime: {round(toc - tic)} s")
