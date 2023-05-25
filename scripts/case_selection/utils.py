from datetime import datetime
from typing import List

import synapseclient
import pandas as pd


def waitifnot(cond, msg=""):
    if not cond:
        for line in msg:
            print(line)
        print("Press control-C to exit and try again.")
        while True:
            pass


def get_default_global(config, key):
    return config["default"]["global"][key]


def get_default_site(config, site, key):
    return config["default"]["site"][site][key]


def get_custom(config, phase, cohort, site, key):
    return config["phase"][phase]["cohort"][cohort]["site"][site][key]


def get_production(config, phase, cohort, site):
    return get_custom(config, phase, cohort, site, "production")


def get_adjusted(config, phase, cohort, site):
    return get_custom(config, phase, cohort, site, "adjusted")


def get_pressure(config, phase, cohort, site):
    n_pressure_default = config["default"]["site"][site]["pressure"]
    n_pressure_specific = config["phase"][phase]["cohort"][cohort]["site"][site][
        "pressure"
    ]
    n_pressure = (
        n_pressure_specific if n_pressure_specific is not None else n_pressure_default
    )
    return n_pressure


def get_sdv_or_irr_value(config, phase, cohort, site, key=["sdv", "irr"]):
    n_pressure = get_pressure(config, phase, cohort, site)
    n_prod = get_production(config, phase, cohort, site)

    # site default
    val_site = get_default_site(config, site, "val")
    if val_site is not None and isinstance(val_site, float) and val_site < 1:
        return round(val_site * n_prod)
    elif val_site is not None:
        return val_site

    # custom
    val_custom = get_custom(config, phase, cohort, site, "val")
    if val_custom is not None and not isinstance(val_custom, float) and val_custom < 1:
        return round(val_custom * n_prod)
    elif val_custom is not None:
        return val_custom

    # site default
    val_site = get_default_site(config, site, "val")
    if val_site is not None and not isinstance(val_site, float) and val_site < 1:
        return round(val_site * n_prod)
    elif val_site is not None:
        return val_site

    # global default
    val_global = get_default_global(config, key)
    if val_global is not None and not isinstance(val_global, float) and val_global < 1:
        return round(val_global * n_prod)
    elif val_global is not None:
        return val_global

    return None


def get_sdv(config, phase, cohort, site):
    return get_sdv_or_irr_value(config, phase, cohort, site, "sdv")


def get_irr(config, phase, cohort, site):
    return get_sdv_or_irr_value(config, phase, cohort, site, "irr")


def now(time_only=False, tz="US/Pacific"):
    if time_only:
        return datetime.now().strftime("%H:%M:%S")
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_synapse_folder_children(
    syn: synapseclient.Synapse,
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
    ents = syn.getChildren(synapse_id, includeTypes=include_types)
    children = {item["name"]: item["id"] for item in ents}
    return children


def get_synid_from_path(syn: synapseclient.Synapse, synid_root: str, path: str) -> str:
    """Get a synapse ID by following a traditional file path from a
    root synapse folder entity.

    Args:
        syn (synapseclient.Synapse): Synapse Connection
        synid_root (str): Synapse id of a folder or project
        path (str): Path of file or folder "first/second/final/file.csv"

    Returns:
        str: Synapse ID of the entity
    """
    synid_current = synid_root
    subfolders = path.split("/")

    for folder in subfolders:
        synid_children = syn.findEntityId(folder, parent=synid_current)
        if synid_children is None:
            return None
        synid_current = synid_children

    return synid_current


def get_synapse_entity_data_in_csv(
    syn: synapseclient.Synapse, synapse_id: str, version: str = None, sep: str = ","
):
    """Download and load data stored in csv or other delimited format
    on Synapse into an R data frame.

    Args:
        syn (synapseclient.Synapse): _description_
        synapse_id (str): _description_
        version (str, optional): _description_. Defaults to None.
        sep (str, optional): _description_. Defaults to ",".

    Returns:
        _type_: _description_
    """
    entity = syn.get(synapse_id, version=version)
    data = pd.read_csv(entity.path, sep=sep)

    return data
