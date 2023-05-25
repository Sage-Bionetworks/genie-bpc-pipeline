#!/usr/bin/env python
# Authors: Xindi Guo, Kristen Dang
# September 17, 2019


import argparse
import os
from datetime import datetime

import synapseclient
import pandas as pd

import utils


def build_args():
    """Build args

    Returns:
        _type_: _description_
    """
    # user input --------------------------
    # TODO Use config file to auto assign choices

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input",
        type=str,
        help="Synapse ID of the input file that has the BPC selected cases",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default="syn20798271",
        help="Synapse ID of the BPC output folder. Default: syn20798271",
    )
    parser.add_argument(
        "-p",
        "--phase",
        type=str,
        help="BPC phase. i.e. pilot, phase 1, phase 1 additional",
        choices=["phase 1", "phase 1 additional", "phase 2"],
    )
    parser.add_argument(
        "-c",
        "--cohort",
        type=str,
        help="BPC cohort. i.e. NSCLC, CRC, BrCa, and etc.",
        choices=["NSCLC", "CRC", "BrCa", "PANC", "Prostate", "BLADDER"],
    )
    parser.add_argument(
        "-s",
        "--site",
        type=str,
        help="BPC site. i.e. DFCI, MSK, UHN, VICC, and etc.",
        choices=["DFCI", "MSK", "UHN", "VICC"],
    )
    args = parser.parse_args()
    return args


def get_latest_consortium_release(syn: synapseclient.Synapse) -> str:
    """Get latest consortium release

    Args:
        syn: Synapse connection

    Returns:
        str: Synapse id of latest consortium release
    """
    consortium_release_folders = syn.tableQuery(
        f"SELECT name, id FROM syn17019650 WHERE "
        "name NOT LIKE 'Release %' "
        "and name NOT LIKE '%-public' "
        "and name NOT IN ('case_lists', 'potential_artifacts')"
        "ORDER BY name"
    )
    consortium_release_folders_df = consortium_release_folders.asDataFrame()
    # Get major release version
    consortium_release_folders_df["major_release"] = [
        int(release.split(".")[0]) for release in consortium_release_folders_df["name"]
    ]
    # only keep the latest consortium release for the public release
    consortium_release_folders_df.drop_duplicates(
        "major_release", keep="last", inplace=True
    )
    sorted_df = consortium_release_folders_df.sort_values(
        "major_release", ascending=False
    )
    print(f"Using {sorted_df['name'].iloc[0]}")
    return sorted_df["id"].iloc[0]


def select_cases(
    syn: synapseclient.Synapse,
    in_file: str,
    out_folder: str,
    phase: str,
    cohort: str,
    site: str,
):
    # TODO: how to get the options
    # file_view_id = "syn21557543"
    # file_view_schema = syn.getColumns(file_view_id)

    # setup ----------------------------
    release_synid = get_latest_consortium_release(syn)

    # clinical data
    # always use the most recent consortium release - Feb 2022
    release_files = utils.get_synapse_folder_children(syn=syn, synapse_id=release_synid)
    clinical_sample_id = release_files["data_clinical_sample.txt"]
    clinical_patient_id = release_files["data_clinical_patient.txt"]

    # mapping tables
    sex_mapping = syn.tableQuery("SELECT * FROM syn7434222").asDataFrame()
    race_mapping = syn.tableQuery("SELECT * FROM syn7434236").asDataFrame()
    ethnicity_mapping = syn.tableQuery("SELECT * FROM syn7434242").asDataFrame()

    # output setup
    # HACK: this is to change "phase 1 additional" into phase1_additional
    # And everything else: "phase 2" into "phase2"
    phase_no_space = phase.replace(" ", "", 1).replace(" ", "_")
    output_entity_name = f"{site}_{cohort}_{phase_no_space}_genie_export.csv"
    today = str(datetime.today()).split(" ")[0]
    output_file_name = f"{site}_{cohort}_{phase_no_space}_genie_export_{today}.csv"

    # download input file and get selected cases/samples
    selected_info = pd.read_csv(syn.get(in_file).path)
    selected_cases = selected_info["PATIENT_ID"]

    # Convert selected_info['SAMPLE_IDS'] to a list
    # This is a column with semi-colon delimited sample ids
    selected_samples = selected_info["SAMPLE_IDS"].str.split(";").explode().tolist()

    # # Create query for selected cases
    # temp = ",".join(selected_samples)
    # temp = [shlex.quote(x) for x in temp.split(",")]

    # Download clinical data
    clinical_sample = pd.read_csv(
        syn.get(clinical_sample_id, followLink=True).path, comment="#", sep="\t"
    )
    # Subset clinical samples to selected samples
    # To reduce the merge
    clinical_sample = clinical_sample[
        clinical_sample["SAMPLE_ID"].isin(selected_samples)
    ]
    clinical_patient = pd.read_csv(
        syn.get(clinical_patient_id, followLink=True).path, comment="#", sep="\t"
    )

    # Combined clinical data
    clinical = pd.merge(clinical_sample, clinical_patient, on="PATIENT_ID", how="left")

    # Change the columns to lowercase
    clinical.columns = clinical.columns.str.lower()

    # Get all samples for those patients
    # TODO: fix me
    # samples_per_patient = clinical['sample_id'][
    #     clinical["patient_id"].isin(selected_cases)
    # ]
    # print(samples_per_patient)

    # Mapping data for each instrument
    patient_output = pd.DataFrame({"record_id": selected_cases})
    patient_output["redcap_repeat_instrument"] = ""
    patient_output["redcap_repeat_instance"] = ""

    patient_output["genie_patient_id"] = patient_output["record_id"]
    patient_output["birth_year"] = clinical.loc[
        clinical["patient_id"].isin(patient_output["genie_patient_id"]), "birth_year"
    ]
    patient_output["naaccr_ethnicity_code"] = clinical.loc[
        clinical["patient_id"].isin(patient_output["genie_patient_id"]), "ethnicity"
    ]
    patient_output["naaccr_race_code_primary"] = clinical.loc[
        clinical["patient_id"].isin(patient_output["genie_patient_id"]), "primary_race"
    ]
    patient_output["naaccr_race_code_secondary"] = clinical.loc[
        clinical["patient_id"].isin(patient_output["genie_patient_id"]),
        "secondary_race",
    ]
    patient_output["naaccr_race_code_tertiary"] = clinical.loc[
        clinical["patient_id"].isin(patient_output["genie_patient_id"]), "tertiary_race"
    ]
    patient_output["naaccr_sex_code"] = clinical.loc[
        clinical["patient_id"].isin(patient_output["genie_patient_id"]), "sex"
    ]

    # Mapping to code
    # TODO: How can you map to code... Its a one to many mapping...
    # you can go from code to cbioportal label but not back
    # Why is it mapped back to code?
    # patient_output["naaccr_ethnicity_code"] = patient_output[
    #     "naaccr_ethnicity_code"
    # ].map(ethnicity_mapping.set_index("CODE")["CBIO_LABEL"])
    # print(patient_output["naaccr_ethnicity_code"])
    # patient_output["naaccr_race_code_primary"] = patient_output[
    #     "naaccr_race_code_primary"
    # ].map(race_mapping.set_index("CBIO_LABEL")["CODE"])
    # patient_output["naaccr_race_code_secondary"] = patient_output[
    #     "naaccr_race_code_secondary"
    # ].map(race_mapping.set_index("CBIO_LABEL")["CODE"])
    # patient_output["naaccr_race_code_tertiary"] = patient_output[
    #     "naaccr_race_code_tertiary"
    # ].map(race_mapping.set_index("CBIO_LABEL")["CODE"])
    # patient_output["naaccr_sex_code"] = patient_output["naaccr_sex_code"].map(
    #     sex_mapping.set_index("CBIO_LABEL")["CODE"]
    # )

    # Recode values in patient_output DataFrame
    patient_output.loc[
        patient_output["birth_year"] == "cannotReleaseHIPAA", "birth_year"
    ] = pd.NA
    # TODO: Fix me
    # patient_output.loc[
    #     patient_output["naaccr_ethnicity_code"] == -1, "naaccr_ethnicity_code"
    # ] = 9
    # patient_output.loc[
    #     patient_output["naaccr_race_code_primary"] == -1, "naaccr_race_code_primary"
    # ] = 99
    # patient_output.loc[
    #     patient_output["naaccr_race_code_secondary"] == -1, "naaccr_race_code_secondary"
    # ] = 88
    # patient_output.loc[
    #     patient_output["naaccr_race_code_tertiary"] == -1, "naaccr_race_code_tertiary"
    # ] = 88

    # Instrument - cancer_panel_test
    sample_info_list = []
    for patient_id in selected_cases:
        sample_list = []
        subset_df = clinical[clinical['PATIENT_ID'] == patient_id]
        # TODO: Fix me
        for i, sample_id in enumerate(x):
            print(sample_id)
            subset_df = clinical[clinical["sample_id"] == sample_id]
            # print(subset_df)
            temp_df = pd.DataFrame(
                {
                    "record_id": subset_df['patient_id']
                }
            )
            temp_df["redcap_repeat_instrument"] = "cancer_panel_test"
            temp_df["redcap_repeat_instance"] = i
            temp_df["redcap_data_access_group"] = subset_df["center"]

            temp_df["cpt_genie_sample_id"] = sample_id
            temp_df["cpt_oncotree_code"] = subset_df["oncotree_code"]
            temp_df["cpt_sample_type"] = subset_df["sample_type_detailed"]
            temp_df["cpt_seq_assay_id"] = subset_df["seq_assay_id"]

            temp_df["cpt_seq_date"] = subset_df["seq_year"]
            temp_df["age_at_seq_report"] = subset_df['age_at_seq_report_days']
            sample_list.append(temp_df)

        combined_df = pd.concat(sample_list, ignore_index=True)
        sample_info_list.append(combined_df)
    sample_info_df = pd.concat(sample_info_list, ignore_index=True)
    patient_output = pd.concat([patient_output, sample_info_df], ignore_index=True)

    # Output and upload
    patient_output.to_csv(output_file_name, index=False)
    act = synapseclient.Activity(
        name="export main GENIE data",
        description="Export selected BPC patient data from main GENIE database",
        used=[clinical_sample_id, clinical_patient_id, in_file],
        executed="https://github.com/Sage-Bionetworks/genie-bpc-pipeline/tree/develop/scripts/case_selection/export_bpc_selected_cases.R",
    )
    syn_file = synapseclient.File(
        output_file_name,
        parent=out_folder,
        name=output_entity_name,
        annotations={"phase": phase, "cohort": cohort, "site": site},
    )
    # syn_file = syn.store(syn_file)
    # syn.setProvenance(syn_file, act)
    # os.remove(output_file_name)


def main(args):
    syn = synapseclient.Synapse()
    syn.login()
    in_file = args.input
    out_folder = args.output
    phase = args.phase
    cohort = args.cohort
    site = args.site
    select_cases(syn, in_file, out_folder, phase, cohort, site)


if __name__ == "__main__":
    args = build_args()
    main(args)
