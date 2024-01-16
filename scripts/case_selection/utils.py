# Python
import synapseclient
import pandas as pd
import os
import time
from typing import List


def waitifnot(cond: bool, msg: List[str] = []):
    if not cond:
        for str in msg:
            print(str)
        print("Press control-C to exit and try again.")
        while True:
            pass


def get_sites_in_config(config: dict, phase: str, cohort: str):
    if phase == "1_additional":
        return list(config["phase"][1]["cohort"][cohort]["site"].keys())
    else:
        return list(config["phase"][phase]["cohort"][cohort]["site"].keys())


def get_default_global(config: dict, key: str):
    return config["default"]["global"][key]


def get_default_site(config: dict, site: str, key: str):
    return config["default"]["site"][site].get(key, None)


def get_custom(config: dict, phase: str, cohort: str, site: str, key: str):
    return config["phase"][phase]["cohort"][cohort]["site"][site].get(key, None)


def get_production(config: dict, phase: str, cohort: str, site: str):
    return get_custom(config, phase, cohort, site, "production")


def get_adjusted(config: dict, phase: str, cohort: str, site: str):
    return get_custom(config, phase, cohort, site, "adjusted")


def get_pressure(config: dict, phase: str, cohort: str, site: str):
    n_pressure_default = config["default"]["site"][site]["pressure"]
    n_pressure_specific = config["phase"][phase]["cohort"][cohort]["site"][site].get(
        "pressure", None
    )
    n_pressure = (
        n_pressure_specific if n_pressure_specific is not None else n_pressure_default
    )
    return n_pressure


def get_sdv_or_irr_value(config: dict, phase: str, cohort: str, site: str, key: str):
    n_pressure = get_pressure(config, phase, cohort, site)
    n_prod = get_production(config, phase, cohort, site)
    val_site = get_default_site(config, site, key)
    if val_site is not None and pd.isna(val_site):
        if val_site < 1:
            return round(val_site * n_prod)
        return val_site
    val_custom = get_custom(config, phase, cohort, site, key)
    if val_custom is not None and not pd.isna(val_custom):
        if val_custom < 1:
            return round(val_custom * n_prod)
        return val_custom
    val_site = get_default_site(config, site, key)
    if val_site is not None and not pd.isna(val_site):
        if val_site < 1:
            return round(val_site * n_prod)
        return val_site
    val_global = get_default_global(config, key)
    if val_global is not None and not pd.isna(val_global):
        if val_global < 1:
            return round(val_global * n_prod)
        return val_global
    return None


def get_sdv(config: dict, phase: str, cohort: str, site: str):
    return get_sdv_or_irr_value(config, phase, cohort, site, "sdv")


def get_irr(config: dict, phase: str, cohort: str, site: str):
    return get_sdv_or_irr_value(config, phase, cohort, site, "irr")


def now(timeOnly: bool = False, tz: str = "US/Pacific"):
    os.environ["TZ"] = tz
    time.tzset()
    if timeOnly:
        return time.strftime("%H:%M:%S", time.localtime())
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def get_synapse_folder_children(
    syn,
    synapse_id: str,
    include_types: List[str] = [
        "folder",
        "file",
        "table",
        "link",
        "entityview",
        "dockerrepo",
    ],
):
    ent = list(syn.getChildren(synapse_id, includeTypes=include_types))
    children = {}
    if len(ent) > 0:
        for i in range(len(ent)):
            children[ent[i]["name"]] = ent[i]["id"]
    return children


def get_folder_synid_from_path(syn, synid_folder_root: str, path: str):
    synid_folder_current = synid_folder_root
    subfolders = path.split("/")
    for i in range(len(subfolders)):
        synid_folder_children = get_synapse_folder_children(
            syn, synid_folder_current, include_types=["folder"]
        )
        if subfolders[i] not in synid_folder_children:
            return None
        synid_folder_current = str(synid_folder_children[subfolders[i]])
    return synid_folder_current


def get_file_synid_from_path(syn, synid_folder_root: str, path: str):
    path_part = path.split("/")
    file_name = path_part[-1]
    path_abbrev = "/".join(path_part[:-1])
    synid_folder_dest = get_folder_synid_from_path(syn, synid_folder_root, path_abbrev)
    synid_folder_children = get_synapse_folder_children(
        syn, synid_folder_dest, include_types=["file"]
    )
    if file_name not in synid_folder_children:
        return None
    return str(synid_folder_children[file_name])


def get_synapse_entity_data_in_csv(
    syn,
    synapse_id: str,
    version: int = None,
    sep: str = ",",
    na_strings: List[str] = ["NA"],
    header: bool = True,
    check_names: bool = True,
):
    if version is None:
        entity = syn.get(synapse_id)
    else:
        entity = syn.get(synapse_id, version=version)
    data = pd.read_csv(entity.path, na_values=na_strings, sep=sep, header=header)
    return data


def validate_argparse_input(config: dict, phase: str, cohort: str, site: str):
    """
    Validate the argparse input.

    Args:
        config (dict): The configuration dictionary.
        phase (str): The phase value.
        cohort (str): The cohort value.
        site (str): The site value.

    Raises:
        ValueError: If the phase, cohort, or site values are invalid.
    """
    # check user input
    # config phases are not all strings
    config_phases = [str(key) for key in config["phase"].keys()]
    phase_str = ", ".join(config_phases)
    if phase not in config_phases:
        raise ValueError(f"Phase {phase} is not valid.  Valid values: {phase_str}")

    cohort_in_config = config["phase"][phase]["cohort"].keys()
    cohort_str = ", ".join(cohort_in_config)
    if cohort not in cohort_in_config:
        raise ValueError(
            f"Cohort {cohort} is not valid for phase {phase}.  Valid values: {cohort_str}"
        )

    sites_in_config = get_sites_in_config(
        config, phase, cohort
    )  # Assuming get_sites_in_config is a function in shared_fxns
    site_str = ", ".join(sites_in_config)
    if site not in sites_in_config:
        raise ValueError(
            f"Site {site} is not valid for phase {phase} and cohort {cohort}.  Valid values: {site_str}"
        )


def clear_synapse_table(syn, table_id):
    res = syn.tableQuery(f"SELECT * FROM {table_id}").asDataFrame()
    tbl = synapseclient.Table(schema=syn.get(table_id), values=res)
    syn.delete(tbl)
    return len(res)


def update_synapse_table(syn, table_id, data):
    entity = syn.get(table_id)
    project_id = entity.properties.parentId
    table_name = entity.properties.name
    table_object = synapseclient.Table(table_name, project_id, data)
    syn.store(table_object)
    return len(data)


def create_synapse_table_version(syn, table_id, data, comment="", append=True):
    if not append:
        _ = clear_synapse_table(syn, table_id)
    _ = update_synapse_table(syn, table_id, data)
    # n_version = snapshot_synapse_table(table_id, comment)
    n_version = syn.create_snapshot_version(table_id, comment=comment)
    return n_version
