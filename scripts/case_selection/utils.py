# Python
import synapseclient
import pandas as pd
import os
import time
from typing import List

syn = synapseclient.Synapse()
syn.login()


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


def get_folder_synid_from_path(synid_folder_root: str, path: str):
    synid_folder_current = synid_folder_root
    subfolders = path.split("/")
    for i in range(len(subfolders)):
        synid_folder_children = get_synapse_folder_children(
            synid_folder_current, include_types=["folder"]
        )
        if subfolders[i] not in synid_folder_children:
            return None
        synid_folder_current = str(synid_folder_children[subfolders[i]])
    return synid_folder_current


def get_file_synid_from_path(synid_folder_root: str, path: str):
    path_part = path.split("/")
    file_name = path_part[-1]
    path_abbrev = "/".join(path_part[:-1])
    synid_folder_dest = get_folder_synid_from_path(synid_folder_root, path_abbrev)
    synid_folder_children = get_synapse_folder_children(
        synid_folder_dest, include_types=["file"]
    )
    if file_name not in synid_folder_children:
        return None
    return str(synid_folder_children[file_name])


def get_synapse_entity_data_in_csv(
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
