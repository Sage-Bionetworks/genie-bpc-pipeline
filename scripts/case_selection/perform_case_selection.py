# Description: Workflow for conducting BPC case selection and constructing both
#   output files representing eligibility criteria, ID lists, and a reports.
# Author: Haley Hunter-Zinck
# Date: 2021-09-22

# pre-setup --------------------------------

import argparse
from datetime import datetime
from random import sample, seed
import time

import numpy as np
import pandas as pd
import synapseclient
import yaml

from utils import *


def get_count_from_percentage(total, perc, min_count=1):
    """
    Calculates and returns the count obtained from a given percentage of a total value.

    Parameters:
        total (int): The total value.
        perc (float): The percentage to calculate the count from.
        min_count (int, optional): The minimum count to be returned. Defaults to 1.

    Returns:
        int: The count obtained from the percentage calculation.
    """
    return max(min_count, round(total * perc / 100))


def get_seq_dates(config, phase, cohort, site):
    """
    Retrieves the sequence dates based on the given configuration, phase, cohort, and site.

    Parameters:
        config (dict): The configuration dictionary.
        phase (str): The phase to retrieve the sequence dates for.
        cohort (str): The cohort to retrieve the sequence dates for.
        site (str): The site to retrieve the sequence dates for.

    Returns:
        list: A list of sequence dates.
    """
    seq_dates = []
    if config["phase"][phase]["cohort"][cohort]["site"][site].get("date") is not None:
        seq_dates = config["phase"][phase]["cohort"][cohort]["site"][site]["date"]
    else:
        seq_dates = config["phase"][phase]["cohort"][cohort]["date"]
    return seq_dates


def get_patient_ids_in_release(syn, synid_file_release):
    """
    Retrieves a list of patient IDs from a specified release file.

    Args:
        syn (Synapse): An instance of the Synapse client.
        synid_file_release (str): The Synapse ID of the release file.

    Returns:
        list: A list of patient IDs extracted from the release file.
    """
    data = pd.read_csv(syn.get(synid_file_release).path)
    return data["record_id"].tolist()


def get_patient_ids_bpc_removed(syn, synid_table_patient_removal, cohort):
    """
    Retrieves the IDs of patients whose blood pressure control has been removed from the specified cohort.

    Parameters:
        syn (Synapse): A Synapse object for accessing the Synapse platform.
        synid_table_patient_removal (str): The name of the table containing patient removal information.
        cohort (str): The name of the cohort to filter by.

    Returns:
        list: A list of record IDs for the removed patients.
    """
    query = (
        f"SELECT record_id FROM {synid_table_patient_removal} WHERE {cohort} = 'true'"
    )
    res = syn.tableQuery(query).asDataFrame()["record_id"].tolist()
    return res


def get_sample_ids_bpc_removed(syn, synid_table_sample_removal, cohort):
    """
    Retrieves a list of sample IDs from a table after removing samples based on a specified cohort.

    Parameters:
        syn (Synapse): An instance of the Synapse client.
        synid_table_sample_removal (str): The ID of the table containing the sample removal information.
        cohort (str): The name of the cohort used for filtering.

    Returns:
        list: A list of sample IDs that meet the specified criteria.
    """
    query = (
        f"SELECT SAMPLE_ID FROM {synid_table_sample_removal} WHERE {cohort} = 'true'"
    )
    res = syn.tableQuery(query).asDataFrame()["SAMPLE_ID"].tolist()
    return res


def get_site_list(site):
    """
    Returns a list of site names based on the input site.

    Parameters:
        site (str): The input site name.

    Returns:
        list: A list of site names. If the input site is "PROV", the function returns ["PROV", "SCI"]. Otherwise, it returns [site].
    """
    if site == "PROV":
        return ["PROV", "SCI"]
    return [site]


def get_eligibility_data(syn, synid_table_patient, synid_table_sample, site):
    """
    Retrieves eligibility data for a given site from the specified tables.

    Parameters:
        syn (Synapse): The Synapse object used for querying the tables.
        synid_table_patient (str): The ID of the table containing patient data.
        synid_table_sample (str): The ID of the table containing sample data.
        site (str): The site for which eligibility data is retrieved.

    Returns:
        pd.DataFrame: The eligibility data for the specified site, containing the following columns:
            - PATIENT_ID: The ID of the patient.
            - SAMPLE_ID: The ID of the sample.
            - ONCOTREE_CODE: The oncotree code.
            - SEQ_DATE: The sequencing date.
            - AGE_AT_SEQ_REPORT: The age of the patient at the sequencing report.
            - SEQ_YEAR: The year of sequencing.
            - YEAR_DEATH: The year of death.
            - INT_CONTACT: The internal contact.
    """
    patient_query = (
        f"SELECT PATIENT_ID, CENTER, YEAR_DEATH, INT_CONTACT FROM {synid_table_patient}"
    )
    patient_data = syn.tableQuery(patient_query).asDataFrame()

    sample_query = f"SELECT PATIENT_ID, SAMPLE_ID, ONCOTREE_CODE, SEQ_DATE, SEQ_YEAR, AGE_AT_SEQ_REPORT FROM {synid_table_sample}"
    sample_data = syn.tableQuery(sample_query).asDataFrame()

    sites = get_site_list(site)

    data = pd.merge(patient_data, sample_data, on="PATIENT_ID")
    data = data[data["CENTER"].isin(sites)]
    data = data[
        [
            "PATIENT_ID",
            "SAMPLE_ID",
            "ONCOTREE_CODE",
            "SEQ_DATE",
            "AGE_AT_SEQ_REPORT",
            "SEQ_YEAR",
            "YEAR_DEATH",
            "INT_CONTACT",
        ]
    ]

    return data


def create_eligibility_matrix(
    data, allowed_codes, seq_min, seq_max, exclude_patient_id=[], exclude_sample_id=[]
):
    """
    Create an eligibility matrix based on the given data.

    Args:
        data (pandas.DataFrame): The input data.
        allowed_codes (List[str]): The list of allowed codes.
        seq_min (str): The minimum sequence date in the format "%b-%Y".
        seq_max (str): The maximum sequence date in the format "%b-%Y".
        exclude_patient_id (List[str], optional): The list of patient IDs to exclude. Defaults to [].
        exclude_sample_id (List[str], optional): The list of sample IDs to exclude. Defaults to [].

    Returns:
        pandas.DataFrame: The eligibility matrix containing the following columns:
            - "PATIENT_ID": The patient ID.
            - "SAMPLE_ID": The sample ID.
            - "ONCOTREE_CODE": The oncotree code.
            - "AGE_AT_SEQ_REPORT": The age at sequence report.
            - "INT_CONTACT": The contact interval.
            - "SEQ_DATE": The sequence date.
            - "SEQ_YEAR": The sequence year.
            - "YEAR_DEATH": The year of death.
            - "SEQ_ALIVE_INT": A boolean flag indicating if the patient is alive at the time of sequence report based on the contact interval.
            - "FLAG_ALLOWED_CODE": A boolean flag indicating if the oncotree code is allowed.
            - "FLAG_ADULT": A boolean flag indicating if the age at sequence report is not "<6570".
            - "FLAG_SEQ_DATE": A boolean flag indicating if the sequence date is within the specified range.
            - "SEQ_ALIVE_YR": A boolean flag indicating if the patient is alive at the time of sequence report based on the year of death.
            - "FLAG_NOT_EXCLUDED": A boolean flag indicating if the patient and sample IDs are not in the exclusion lists.
    """
    data["FLAG_ALLOWED_CODE"] = data["ONCOTREE_CODE"].isin(allowed_codes)
    data["FLAG_ADULT"] = data["AGE_AT_SEQ_REPORT"] != "<6570"
    seq_dates = data["SEQ_DATE"].apply(
        lambda seq_date: datetime.strptime(seq_date, "%b-%Y")
    )
    seq_max_datetime = datetime.strptime(seq_max, "%b-%Y")
    seq_min_datetime = datetime.strptime(seq_min, "%b-%Y")
    data["FLAG_SEQ_DATE"] = (seq_dates >= seq_min_datetime) & (
        seq_dates <= seq_max_datetime
    )
    # TODO: These to_numeric fields pose a risk
    death_year = pd.to_numeric(data["YEAR_DEATH"], errors="coerce")
    data["SEQ_ALIVE_YR"] = death_year.notna() | (death_year >= data["SEQ_YEAR"])
    int_contact = pd.to_numeric(data["YEAR_DEATH"], errors="coerce")
    int_age_at_seq_report = pd.to_numeric(data["AGE_AT_SEQ_REPORT"], errors="coerce")
    data["SEQ_ALIVE_INT"] = int_age_at_seq_report.notna() | (
        int_contact >= int_age_at_seq_report
    )
    data["FLAG_NOT_EXCLUDED"] = ~data["PATIENT_ID"].isin(exclude_patient_id) & ~data[
        "SAMPLE_ID"
    ].isin(exclude_sample_id)
    return data[
        [
            "PATIENT_ID",
            "SAMPLE_ID",
            "ONCOTREE_CODE",
            "AGE_AT_SEQ_REPORT",
            "INT_CONTACT",
            "SEQ_DATE",
            "SEQ_YEAR",
            "YEAR_DEATH",
            "SEQ_ALIVE_INT",
            "FLAG_ALLOWED_CODE",
            "FLAG_ADULT",
            "FLAG_SEQ_DATE",
            "SEQ_ALIVE_YR",
            "FLAG_NOT_EXCLUDED",
        ]
    ]


def get_eligible_cohort(
    x: pd.DataFrame, phase: str, site: str, cohort: str, randomize: bool = True
):
    """
    Generates a cohort of eligible patients based on specified criteria.

    Parameters:
        x (DataFrame): The input DataFrame containing patient data.
        phase (str): The phase of the study.
        site (str): The study site.
        cohort (str): The study cohort.
        randomize (bool, optional): Whether to randomize the order of eligible patients. Defaults to True.

    Returns:
        DataFrame: A DataFrame containing the order, patient ID, and sample IDs of eligible patients.
    """
    col_flags = [col for col in x.columns if col.startswith("FLAG_")]
    x["flag_eligible"] = x[col_flags].all(axis=1)
    eligible = (
        x[x["flag_eligible"]]
        .groupby("PATIENT_ID")["SAMPLE_ID"]
        .apply(lambda x: ";".join(x))
        .reset_index()
        .rename(columns={"SAMPLE_ID": "SAMPLE_IDS"})
    )
    if eligible.empty:
        raise ValueError(
            f"Number of eligible samples for phase {phase} {site} {cohort} is 0.  Please revise eligibility criteria."
        )
    if randomize:
        eligible = eligible.sample(frac=1).reset_index(drop=True)
    eligible["order"] = range(1, len(eligible) + 1)
    return eligible[["order", "PATIENT_ID", "SAMPLE_IDS"]]


def create_selection_matrix(
    eligible_cohort,
    n_prod,
    n_pressure,
    n_sdv,
    n_irr,
    phase: str,
    site: str,
    cohort: str,
):
    """
    Generate the selection matrix for eligible patients based on the given parameters.

    Args:
        eligible_cohort (pandas.DataFrame): A DataFrame containing the eligible cohort data.
        n_prod (int): The number of patients needed for production target.
        n_pressure (int): The number of patients needed for pressure.
        n_sdv (int): The number of patients needed for SDV.
        n_irr (int): The number of patients needed for IRR.
        phase (str): The phase of the study.
        site (str): The site of the study.
        cohort (str): The cohort of the study.

    Returns:
        pandas.DataFrame: The selection matrix containing the order, patient ID, sample IDs, pressure, SDV, IRR, and category columns.
    """
    n_eligible = len(eligible_cohort)
    if n_eligible < n_prod:
        raise ValueError(
            f"not enough eligible patients for production target ({n_eligible} < {n_prod}) for phase {phase} {site} {cohort}.  Please revise eligibility criteria."
        )
    seed(site_seed)
    col_sdv = [""] * n_eligible
    idx_sdv = sample(range(n_pressure, n_prod), n_sdv)
    for i in range(n_pressure):
        col_sdv[i] = "sdv"
    for i in idx_sdv:
        col_sdv[i] = "sdv"
    col_irr = [""] * n_eligible
    idx_irr = sample(list(set(range(n_pressure, n_prod)) - set(idx_sdv)), n_irr)
    for i in idx_irr:
        col_irr[i] = "irr"
    eligible_cohort["pressure"] = ["pressure"] * n_pressure + [""] * (
        n_eligible - n_pressure
    )
    eligible_cohort["sdv"] = col_sdv
    eligible_cohort["irr"] = col_irr
    eligible_cohort["category"] = ["production"] * n_prod + ["extra"] * (
        n_eligible - n_prod
    )
    return eligible_cohort[
        ["order", "PATIENT_ID", "SAMPLE_IDS", "pressure", "sdv", "irr", "category"]
    ]


def build_parser():
    """
    Builds and returns a parser object for command line arguments.

    Returns:
        dict: A dictionary containing the parsed command line arguments, including the BPC phase, cohort, site, and config.

    Raises:
        AssertionError: If the provided phase, cohort, or site values are not valid according to the config file.

    Example:
        >>> build_parser()
        {"phase": "example_phase", "cohort": "example_cohort", "site": "example_site", "config": {"example_key": "example_value"}}
    """
    # parameters
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--phase", help="BPC phase", type=str)
    parser.add_argument("-c", "--cohort", help="BPC cohort")
    parser.add_argument("-s", "--site", help="BPC site")
    args = parser.parse_args()

    phase = args.phase
    cohort = args.cohort
    site = args.site

    validate_argparse_input(config=config, phase=phase, cohort=cohort, site=site)

    return {"phase": phase, "cohort": cohort, "site": site, "config": config}


def main(config, phase, cohort, site):
    """
    Generates a function comment for the given function body.

    Parameters:
    - config: The configuration object.
    - phase: The phase of the process.
    - cohort: The cohort being analyzed.
    - site: The site being analyzed.
    """
    # additional parameters
    flag_additional = "addition" in phase

    if not flag_additional:
        if (
            get_production(config, phase, cohort, site) == 0
        ):  # Assuming get_production is a function in shared_fxns
            raise ValueError(
                f"Production target is 0 for phase {phase} {site} {cohort}.  Please revise eligibility criteria."
            )

    syn = synapseclient.login()

    tic = time.time()

    # set random seed
    default_site_seed = config["default"]["site"][site]["seed"]
    cohort_site_seed = config["phase"][phase]["cohort"][cohort]["site"][site].get(
        "seed", None
    )
    global site_seed
    site_seed = cohort_site_seed if cohort_site_seed is not None else default_site_seed
    np.random.seed(site_seed)

    # output files
    file_matrix = f"{cohort}_{site}_phase{phase}_eligibility_matrix.csv".lower()
    file_selection = f"{cohort}_{site}_phase{phase}_case_selection.csv".lower()
    file_add = f"{cohort}_{site}_phase{phase}_samples.csv".lower()

    # misc parameters
    debug = config["misc"]["debug"]

    if debug:
        print(f"{datetime.now().time()}: querying data to determine eligibility...")

    eligibility_data = get_eligibility_data(
        syn=syn,
        synid_table_patient=config["synapse"]["main_patient"]["id"],
        synid_table_sample=config["synapse"]["main_sample"]["id"],
        site=site,
    )

    if debug:
        print(f"{datetime.now().time()}: calculating eligibility criteria...")

    exclude_patient_id = []
    exclude_sample_id = []
    seq_dates = get_seq_dates(config, phase, cohort, site)

    flag_prev_release = (
        config["release"]["cohort"][cohort]["patient_level_dataset"] != "NA"
    )
    if phase == 2 and flag_prev_release:
        exclude_patient_id = get_patient_ids_in_release(
            syn=syn,
            synid_file_release=config["release"]["cohort"][cohort][
                "patient_level_dataset"
            ],
        )
        exclude_patient_id += get_patient_ids_bpc_removed(
            syn=syn,
            synid_table_patient_removal=config["synapse"]["bpc_removal_patient"]["id"],
            cohort=cohort,
        )
        exclude_sample_id = get_sample_ids_bpc_removed(
            syn=syn,
            synid_table_sample_removal=config["synapse"]["bpc_removal_sample"]["id"],
            cohort=cohort,
        )

    eligibility_matrix = create_eligibility_matrix(
        data=eligibility_data,
        allowed_codes=config["phase"][phase]["cohort"][cohort]["oncotree"][
            "allowed_codes"
        ],
        seq_min=seq_dates["seq_min"],
        seq_max=seq_dates["seq_max"],
        exclude_patient_id=exclude_patient_id,
        exclude_sample_id=exclude_sample_id,
    )

    if debug:
        print(f"{datetime.now().time()}: extracting eligible patient IDs...")

    eligible_cohort = get_eligible_cohort(
        x=eligibility_matrix, cohort=cohort, site=site, phase=phase, randomize=True
    )

    if debug:
        print(f"{datetime.now().time()}: conducting case selection...")

    # assign case selection categories
    if flag_additional:
        query = f"SELECT record_id AS PATIENT_ID FROM {config['synapse']['bpc_patient']['id']} WHERE cohort = '{cohort}' AND redcap_data_access_group = '{site}'"
        bpc_pat_ids = syn.tableQuery(
            query, includeRowIdAndRowVersion=False
        ).asDataFrame()

        added_sam = eligible_cohort[
            eligible_cohort["PATIENT_ID"].isin(bpc_pat_ids["PATIENT_ID"])
        ][["PATIENT_ID", "SAMPLE_IDS"]]

        added_sam["ALREADY_IN_BPC"] = False
        if len(added_sam) > 0:
            for i in range(len(added_sam)):
                ids_sam = added_sam.iloc[i]["SAMPLE_IDS"]
                str_ids_sam = "','".join(ids_sam.split(";"))
                query = f"SELECT cpt_genie_sample_id FROM {config['synapse']['bpc_sample']['id']} WHERE cpt_genie_sample_id IN ({str_ids_sam})"
                res = syn.tableQuery(
                    query, includeRowIdAndRowVersion=False
                ).asDataFrame()
                if len(res) > 0:
                    added_sam["ALREADY_IN_BPC"].iloc[i] = True
    else:
        case_selection = create_selection_matrix(
            eligible_cohort=eligible_cohort,
            n_prod=get_production(config, phase, cohort, site),
            n_pressure=get_pressure(config, phase, cohort, site),
            n_sdv=get_sdv(config, phase, cohort, site),
            n_irr=get_irr(config, phase, cohort, site),
            phase=phase,
            site=site,
            cohort=cohort,
        )

    # write locally -----------------------

    if debug:
        print(
            f"{datetime.now().time()}: writing eligibility matrix and case selection to file..."
        )

    if flag_additional:
        added_sam.to_csv(file_add, index=False)
    else:
        eligibility_matrix.to_csv(file_matrix, index=False)
        case_selection.to_csv(file_selection, index=False)

    # close out ----------------------------

    if debug and flag_additional:
        print("Summary:")
        print(f"  Phase: {phase}")
        print(f"  Cohort: {cohort}")
        print(f"  Site: {site}")
        print(f"  Total number of additional samples: {len(added_sam)}")
        print(f"Outfile: {file_add}")
    else:
        print("Summary:")
        print(f"  Phase: {phase}")
        print(f"  Cohort: {cohort}")
        print(f"  Site: {site}")
        print(f"  Total number of samples: {len(eligibility_data)}")
        print(f"  Number of eligible patients: {len(eligible_cohort)}")
        print(
            f"  Number of target cases: {get_production(config, phase, cohort, site)}"
        )
        print(
            f"  Number of pressure cases: {get_pressure(config, phase, cohort, site)}"
        )
        print(
            f"  Number of SDV cases (excluding pressure): {get_sdv(config, phase, cohort, site)}"
        )
        print(f"  Number of IRR cases: {get_irr(config, phase, cohort, site)}")
        print(f"Outfiles: {file_matrix}, {file_selection}")

    toc = time.time()

    print(f"Runtime: {round(toc - tic)} s")


if __name__ == "__main__":
    configuration = build_parser()
    main(
        config=configuration["config"],
        phase=configuration["phase"],
        cohort=configuration["cohort"],
        site=configuration["site"],
    )
