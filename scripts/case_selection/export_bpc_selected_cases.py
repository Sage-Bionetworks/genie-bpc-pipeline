#!/usr/bin/env python
# Authors: Xindi Guo, Kristen Dang
# September 17, 2019

import argparse
import pandas as pd
import synapseclient
import os
from synapseclient import Synapse
from synapseclient import Activity, File
import numpy as np
from datetime import date

# user input --------------------------
parser = argparse.ArgumentParser()
parser.add_argument(
    "-i",
    "--input",
    help="Synapse ID of the input file that has the BPC selected cases",
    required=True,
)
parser.add_argument(
    "-o",
    "--output",
    default="syn20798271",
    help="Synapse ID of the BPC output folder. Default: syn20798271",
)
parser.add_argument(
    "-p",
    "--phase",
    help="BPC phase. i.e. pilot, phase 1, phase 1 additional",
    required=True,
)
parser.add_argument(
    "-c", "--cohort", help="BPC cohort. i.e. NSCLC, CRC, BrCa, and etc.", required=True
)
parser.add_argument(
    "-s", "--site", help="BPC site. i.e. DFCI, MSK, UHN, VICC, and etc.", required=True
)
args = parser.parse_args()

in_file = args.input
out_folder = args.output
phase = args.phase
cohort = args.cohort
site = args.site

# check user input -----------------
# TODO why are the inputs different...
phase_option = ["phase 1", "phase 1 additional", "phase 2"]
cohort_option = ["NSCLC", "CRC", "BrCa", "PANC", "Prostate", "BLADDER"]
site_option = ["DFCI", "MSK", "UHN", "VICC"]

assert (
    phase in phase_option
), f"Error: {phase} is not a valid phase. Valid values: {', '.join(phase_option)}"
assert (
    cohort in cohort_option
), f"Error: {cohort} is not a valid cohort. Valid values: {', '.join(cohort_option)}"
assert (
    site in site_option
), f"Error: {site} is not a valid site. Valid values: {', '.join(site_option)}"

# setup --------------------------
syn = Synapse()
syn.login()

# clinical data
# always use the most recent consortium release - Feb 2022
clinical_sample_id = "syn9734573"
clinical_patient_id = "syn9734568"
# HACK pin version
clinical_sample_id = "syn51499964"
clinical_patient_id = "syn51499962"

# mapping tables
sex_mapping = syn.tableQuery("SELECT * FROM syn7434222").asDataFrame()
race_mapping = syn.tableQuery("SELECT * FROM syn7434236").asDataFrame()
ethnicity_mapping = syn.tableQuery("SELECT * FROM syn7434242").asDataFrame()

# output setup
phase_no_space = phase.replace(" ", "_")
output_entity_name = f"{site}_{cohort}_{phase_no_space}_genie_export.csv"
output_file_name = f"{site}_{cohort}_{phase_no_space}_genie_export_{date.today()}.csv"

# download input file and get selected cases/samples
selected_info = pd.read_csv(syn.get(in_file).path)
selected_cases = selected_info["PATIENT_ID"]
selected_samples = ";".join(selected_info["SAMPLE_IDS"]).split(";")

# create the data file ----------------------------

# Create query for selected cases
temp = ", ".join([f"'{item}'" for item in selected_samples])

# download clinical data
# sample clinical data
clinical_sample = pd.read_csv(
    syn.get(clinical_sample_id, followLink=True).path,
    comment="#",
    sep="\t",
)
clinical_sample = clinical_sample[clinical_sample["SAMPLE_ID"].isin(selected_samples)]

# patient clinical data
clinical_patient = pd.read_csv(
    syn.get(clinical_patient_id, followLink=True).path,
    comment="#",
    sep="\t",
)

# combined clinical data
clinical = pd.merge(clinical_sample, clinical_patient, on="PATIENT_ID", how="left")

# change the columns to lower case
clinical.columns = map(str.lower, clinical.columns)

# Get all samples for those patients
samples_per_patient = [
    clinical["sample_id"][clinical["patient_id"] == x].tolist() for x in selected_cases
]


def remap_clinical_values(
    clinicaldf: pd.DataFrame,
    sex_mapping: pd.DataFrame,
    race_mapping: pd.DataFrame,
    ethnicity_mapping: pd.DataFrame,
) -> pd.DataFrame:
    """Remap clinical attributes from integer to string values

    Args:
        clinicaldf: Clinical data
        sex_mapping: Sex mapping data
        race_mapping: Race mapping data
        ethnicity_mapping: Ethnicity mapping data

    Returns:
        Mapped clinical dataframe
    """

    race_mapping.index = race_mapping["CBIO_LABEL"]
    race_dict = race_mapping.to_dict()

    ethnicity_mapping.index = ethnicity_mapping["CBIO_LABEL"]
    ethnicity_dict = ethnicity_mapping.to_dict()

    sex_mapping.index = sex_mapping["CBIO_LABEL"]
    sex_dict = sex_mapping.to_dict()

    # Use pandas mapping feature
    clinicaldf = clinicaldf.replace(
        {
            "PRIMARY_RACE": race_dict["CODE"],
            "SECONDARY_RACE": race_dict["CODE"],
            "TERTIARY_RACE": race_dict["CODE"],
            "SEX": sex_dict["CODE"],
            "ETHNICITY": ethnicity_dict["CODE"],
        }
    )

    return clinicaldf


subset_patient = clinical_patient[clinical_patient["PATIENT_ID"].isin(selected_cases)]
# TODO: these mappings go from non-granular to granular CODES
# TODO which _could be_ an issue.  Check about these mappings..
subset_patient = remap_clinical_values(
    clinicaldf=subset_patient,
    sex_mapping=sex_mapping,
    race_mapping=race_mapping,
    ethnicity_mapping=ethnicity_mapping,
)
subset_patient.columns = map(str.lower, subset_patient.columns)

# mapping data for each instrument
# instrument - patient_characteristics
patient_output = pd.DataFrame({"record_id": selected_cases})
patient_output["redcap_repeat_instrument"] = ""
patient_output["redcap_repeat_instance"] = ""

patient_output["genie_patient_id"] = patient_output["record_id"]
patient_output.set_index("genie_patient_id", inplace=True, drop=False)
patient_output.loc[subset_patient["patient_id"], "birth_year"] = subset_patient[
    "birth_year"
].to_list()
patient_output.loc[
    subset_patient["patient_id"], "naaccr_ethnicity_code"
] = subset_patient["ethnicity"].to_list()
patient_output.loc[
    subset_patient["patient_id"], "naaccr_race_code_primary"
] = subset_patient["primary_race"].to_list()
patient_output.loc[
    subset_patient["patient_id"], "naaccr_race_code_secondary"
] = subset_patient["secondary_race"].to_list()
patient_output.loc[
    subset_patient["patient_id"], "naaccr_race_code_tertiary"
] = subset_patient["tertiary_race"].to_list()
patient_output.loc[subset_patient["patient_id"], "naaccr_sex_code"] = subset_patient[
    "sex"
].to_list()

# recode
# cannotReleaseHIPAA = NA
patient_output["birth_year"].replace("cannotReleaseHIPAA", np.nan, inplace=True)
# -1 Not collected = 9 Unknown
patient_output["naaccr_ethnicity_code"].replace(-1, 9, inplace=True)
# -1 Not collected = 99 Unknown
patient_output["naaccr_race_code_primary"].replace(-1, 99, inplace=True)
# -1 Not collected = 88 according to NAACCR
patient_output["naaccr_race_code_secondary"].replace(-1, 88, inplace=True)
patient_output["naaccr_race_code_tertiary"].replace(-1, 88, inplace=True)

# instrument - cancer_panel_test
sample_info_list = []
for samples in samples_per_patient:
    sample_list = []
    for i, sample in enumerate(samples, start=1):
        temp_df = pd.DataFrame(
            {"record_id": clinical["patient_id"][clinical["sample_id"] == sample]}
        )
        temp_df["redcap_repeat_instrument"] = "cancer_panel_test"
        temp_df["redcap_repeat_instance"] = i
        temp_df["redcap_data_access_group"] = clinical["center"][
            clinical["sample_id"] == sample
        ]

        temp_df["cpt_genie_sample_id"] = sample
        temp_df["cpt_oncotree_code"] = clinical["oncotree_code"][
            clinical["sample_id"] == sample
        ]
        temp_df["cpt_sample_type"] = clinical["sample_type_detailed"][
            clinical["sample_id"] == sample
        ]
        temp_df["cpt_seq_assay_id"] = clinical["seq_assay_id"][
            clinical["sample_id"] == sample
        ]
        temp_df["cpt_seq_date"] = clinical["seq_year"][clinical["sample_id"] == sample]
        temp_df["age_at_seq_report"] = clinical["age_at_seq_report_days"][
            clinical["sample_id"] == sample
        ]
        sample_list.append(temp_df)
    combined_df = pd.concat(sample_list)
    sample_info_list.append(combined_df)

sample_info_df = pd.concat(sample_info_list)
patient_output = pd.concat([patient_output, sample_info_df])

# output and upload ----------------------------
from genie import process_functions


with open(output_file_name, "w") as out_f:
    no_float_text = process_functions.removePandasDfFloat(patient_output)
    convert_to_csv = no_float_text.replace("\t", ",")
    out_f.write(convert_to_csv)
# patient_output.to_csv(output_file_name, index=False, na_rep="")

# create an Activity
act = Activity(
    name="export main GENIE data",
    description="Export selected BPC patient data from main GENIE database",
    used=[clinical_sample_id, clinical_patient_id, in_file],
    executed="https://github.com/Sage-Bionetworks/genie-bpc-pipeline/tree/develop/scripts/case_selection/export_bpc_selected_cases.R",
)

# create a File
syn_file = File(
    path=output_file_name,
    parent=out_folder,
    name=output_entity_name,
    annotations={"phase": phase, "cohort": cohort, "site": site},
)

# TODO add this back in
# # store the File in Synapse
# syn_file = syn.store(syn_file)

# # set the provenance for the File
# syn.setProvenance(syn_file, act)

# remove the local file
# os.remove(output_file_name)
