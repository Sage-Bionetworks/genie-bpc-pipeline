BPC Table Update
================
[![automated](https://img.shields.io/docker/cloud/automated/sagebionetworks/genie-bpc-pipeline-table-updates)](https://hub.docker.com/r/sagebionetworks/genie-bpc-pipeline-table-updates)
![status](https://img.shields.io/docker/cloud/build/sagebionetworks/genie-bpc-pipeline-table-updates)


This folder contains multiple scripts to update the tables required for the BPC pipeline.  Note there are two separate update scripts right now
that may require different synapseclient versions. We encourage using different conda environments for each script.

# Service catalog instance
Use a t3.2xlarge ec2 instance for large memory requirement.

# Synapse Credential
Please make sure you have the [.synapseConfig file](https://help.synapse.org/docs/Client-Configuration.1985446156.html)


# Update data element catalog + table schema

## Installation and Setup

### Python version
Make sure you have Python 3.8 and conda installed.

### Install the required packages
```
pip install 'synapseclient[pandas] == 2.7.2'
```

> [!NOTE]  
> Due to this tool using an older version of the python client, until there is bandwidth to update
> please do `rm -rf ~/.synapseCache/*` to clear the synapse cache to avoid this error
> ```
> if cached_time.endswith(".000Z"):
> AttributeError: 'dict' object has no attribute 'endswith'
> ```

```
# Make sure you have anaconda installed
conda create -n genie-table-update-precursor python=3.8
conda activate genie-table-update-precursor
pip install 'synapseclient[pandas] == 2.7.2'
```

### Usage

Prepare the Synapse tables to be updated
> **_NOTE:_** ONLY need to be executed when there is a new version of PRISSMM data dictionary

##### Step 1. Update the Data Catalog
    python update_data_element_catalog.py -v [prissmm_version_number]
##### Step 2. Update the table schema
    python update_table_schema.py


# Update Data Table

The `update_data_table.py` script is used to update the BPC internal tables. 

## Installation and Setup

### Python version
Make sure you have Python 3.9+ and conda installed.

### Install the required packages

```
pip install -r requirements.txt
```

```
conda create -n genie-table-update python=3.10
conda activate genie-table-update
pip install -r requirements.txt
```

### Usage
Update the Synapse Tables with data

#### Primary Case Tables
    python update_data_table.py -m [version_comment] primary
#### IRR Case Tables
    python update_data_table.py -m [version_comment] irr

This is to run the script manually, there is a nextflow workflow associated with this script.
