#!/usr/bin/env python3

import os
import json
import time
import argparse

import synapseclient
from synapseclient import Synapse
from synapseclient.core.exceptions import (
    SynapseAuthenticationError,
    SynapseNoCredentialsError,
)


def get_auth_token() -> str:
    """Get Synapse personal access token from environmental
    variables, if available.

    Returns:
        str: Synapse personal access token or None
    """
    auth_token = None
    if os.getenv("SYNAPSE_AUTH_TOKEN") is not None:
        auth_token = os.getenv("SYNAPSE_AUTH_TOKEN")
    elif os.getenv("SCHEDULED_JOB_SECRETS") is not None:
        secrets = json.loads(os.getenv("SCHEDULED_JOB_SECRETS"))
        auth_token = secrets["SYNAPSE_AUTH_TOKEN"]

    return auth_token


def synapse_login(synapse_config=synapseclient.client.CONFIG_FILE):
    """Login to Synapse.  Looks for Synapse credentials in the following order:
    (1) SYNAPSE_AUTH_TOKEN environmental variable
    (2) SCHEDULED_JOB_SECRETS environmental variable (contains SYNAPSE_AUTH_TOKEN
        in JSON string)
    (3) configuration file

    Args:
        synapse_config: Path to synapse configuration file.
                        Defaults to ~/.synapseConfig

    Returns:
        Synapse connection
    """
    try:
        syn = synapseclient.Synapse(skip_checks=True, configPath=synapse_config)
        auth_token = get_auth_token()
        if auth_token is not None:
            syn.login(silent=True, authToken=auth_token)
        else:
            syn.login(silent=True)
    except (SynapseNoCredentialsError, SynapseAuthenticationError):
        raise ValueError(
            "Login error: please make sure you have correctly "
            "configured your client."
        )
    return syn


def get_user_ids(syn: Synapse, users: list = None):
    """Get users ids from list of user ids or usernames.  This will also
    confirm that the users specified exist in the system

    Args:
        syn: Synapse connection
        users: List of Synapse user Ids or usernames

    Returns:
        List of Synapse user Ids.
    """
    if users is None:
        user_ids = [syn.getUserProfile()["ownerId"]]
    else:
        user_ids = [syn.getUserProfile(user)["ownerId"] for user in users]
    return user_ids


def main():
    """Invoke"""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--synapse_config",
        metavar="file",
        type=str,
        default=synapseclient.client.CONFIG_FILE,
        help="Synapse config file with user credentials: (default %(default)s)",
    )
    args = parser.parse_args()

    syn = synapse_login(args.synapse_config)
    syn.sendMessage(
                get_user_ids(syn),
                "test nextflow",
                f"{time.strftime('%H:%M:%S')} this email has been sent via the synapseclient",
                contentType="text/html",
            )


if __name__ == "__main__":
    main()
