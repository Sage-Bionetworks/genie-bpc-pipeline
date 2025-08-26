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

###### Example 1: Update the data element catalog using the data dictionary:
```
python update_data_element_catalog.py dd -v [prissmm_version_number]
```

###### Example 2: Update the data element catalog using the scope of release (not in use):
```
python update_data_element_catalog.py sor
```

###### Example 3: Dry run for data dictionary update:
```
python update_data_element_catalog.py --dry_run dd -v v3.1.1
```

##### Step 2. Update the table schema

```
python update_table_schema.py
```

# Update Data Table

The `update_data_table.py` script is used to update the BPC internal tables. 

## Installation and Setup

### Python version
Make sure you have Python 3.9+ and conda installed.

### Install the required packages

```
conda create -n genie-table-update python=3.10
conda activate genie-table-update
pip install -r requirements.txt
```

### Usage
Update the Synapse Tables with data

#### Primary Case Tables
##### 1. dry-run (Save output to local)
    python update_data_table.py -c [cohort_name] -m [version_comment] primary -d

##### 2. production (Save output to production projects)
    python update_data_table.py -c [cohort_name] -m [version_comment] primary -pd

##### 3. staging (Save output to staging projects)
    python update_data_table.py -c [cohort_name] -m [version_comment] primary

##### 4. replace tier1a variables in patient_characteristics and cancer_panel_test tables
Once the column mappings are confirmed for both the cohort and the main GENIE release version in [GENIE BPC Elements Mapping](https://www.synapse.org/Synapse:syn20945902/tables/), proceed to run

    python update_data_table.py -c [cohort_name] -m [version_comment] primary -rs true -rp true

#### IRR Case Tables (Deprecated)
    python update_data_table.py -m [version_comment] irr

This is to run the script manually, there is a nextflow workflow associated with this script.
