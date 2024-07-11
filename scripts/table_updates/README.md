BPC Table Update
================
[![automated](https://img.shields.io/docker/cloud/automated/sagebionetworks/genie-bpc-pipeline-table-updates)](https://hub.docker.com/r/sagebionetworks/genie-bpc-pipeline-table-updates)
![status](https://img.shields.io/docker/cloud/build/sagebionetworks/genie-bpc-pipeline-table-updates)

Installation and Setup
----------------------
### Python version
Make sure you have Python 3.8 installed


### Install the required packages
    (sudo) pip install -r requirements.txt

### Synapse Credential
Please make sure you have the [.synapseConfig file](https://help.synapse.org/docs/Client-Configuration.1985446156.html)

### Service catalog instance
Use a t3.2xlarge ec2 instance for large memory requirement.

Putting it all together.

```
# Make sure you have anaconda installed
conda create -n genie-table-update python=3.8
conda activate genie-table-update
pip install -r requirements.txt
```

Usage
-----
### Prepare the Synapse tables to be updated
> **_NOTE:_** ONLY need to be executed when there is a new version of PRISSMM data dictionary

##### Step 1. Update the Data Catalog
    python update_data_element_catalog.py -v [prissmm_version_number]
##### Step 2. Update the table schema
    python update_table_schema.py

### Update the Synapse Tables with data
#### Primary Case Tables
    python update_data_table.py -m [version_comment] primary
#### IRR Case Tables
    python update_data_table.py -m [version_comment] irr
