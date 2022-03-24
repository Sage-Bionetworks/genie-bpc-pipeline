# Synthetic BPC data for pipeline testing

## Overview
Creates synthetic REDCap datasets for six cohorts using REDCap data dictionaries.  These datasets are then uncoded and merged by cohort.  Finally, the uncoded cohort datasets are uploaded to Synapse tables.  

## Installation

Clone this repository and navigate to the directory:
```
git clone git@github.com:Sage-Bionetworks/genie-bpc-pipeline.git
cd genie-bpc-pipeline/scripts/synth
```

Install required R packages:
```
R -e 'renv::restore()'
```

Install required python packages:
```
pip install -r requirements.txt
```

## Synapse credentials

Cache your Synapse personal access token (PAT) as an environmental variable:
```
export SYNAPSE_AUTH_TOKEN={your_personal_access_token_here}
```

or store in ~/.synapseConfig with the following format:
```
[authentication]

# either authtoken OR username and password
authtoken = {your_personal_access_token_here}
```

## Usage

To run the full workflow:

```
sh workflow_synthetic_bpc.sh
```

### Synthetic REDCap datasets
To create synthetic REDCap datasets from a data dictionary: 
```
Rscript synthetic_generate_redcap_data.R -h
```

```
Usage: synthetic_generate_redcap_data.R [options]


Options:
	-d SYNID_FILE_DD, --synid_file_dd=SYNID_FILE_DD
		Synapse ID of REDCap data dictionary

	-c COHORT, --cohort=COHORT
		Name of the cohort to generate (default: synth_cohort)

	-n N_PATIENT, --n_patient=N_PATIENT
		Number of synthetic patients to generate (default: 10)

	-s SITE, --site=SITE
		Name of the site associated with the synthetic dataset (default: synth_site)

	-p RECORD_PREFIX, --record_prefix=RECORD_PREFIX
		Prefix to each synthetic record_id (default: patient)

	-r NON_REPEATING_FORMS, --non_repeating_forms=NON_REPEATING_FORMS
		Name of the site associated with the synthetic dataset (default: curation_completion;curation_initiation_eligibility;patient_characteristics;quality_assurance)

	-u UPLOAD, --upload=UPLOAD
		Upload to synapse ID folder (default: NULL)

	-v, --verbose
		Output messaging to user on script progress (default: FALSE)

	-h, --help
		Show this help message and exit
```

Example run: 
```
Rscript synthetic_generate_redcap_data.R  -d syn26469280 -c 'BPC Bladder Cancer' -n 3 -s SAGE -v -p GENIE
```

### Merge and uncode synthetic REDCap datasets
To merge and uncode the synthetic REDCap datasets: 
```
Rscript synthetic_merge_and_uncode_rca.R -h
```

```
Usage: synthetic_merge_and_uncode_rca.R [options]


Options:
	-f SYNID_FILE_LIST, --synid_file_list=SYNID_FILE_LIST
		Comma separated list of Synapse IDs

	-d SYNID_FILE_DD, --synid_file_dd=SYNID_FILE_DD
		Synapse ID of data dictionary file

	-s SYNID_FOLDER_DEST, --synid_folder_dest=SYNID_FOLDER_DEST
		Synapse ID of folder in which to save the final dataset

	-b, --bpc
		Perform GENIE BPC specific hacks to merged dataset

	-o OUTPUT_PREFIX, --output_prefix=OUTPUT_PREFIX
		Prefix to append to output file name

	-v, --verbose
		Output messaging to user on script progress

	-h, --help
		Show this help message and exit
```

Example run: 
```
Rscript synthetic_merge_and_uncode_rca.R -f syn27541023 -d syn26469280 -s syn26469947 -o 'bladder' -b -v
```

### Load synthetic REDCap data to Synapse tables
To load the synthetic REDCap data to Synapse tables: 
```
python synthetic_update_data_table.py -h
```

```
usage: synthetic_update_data_table.py [-h] [-s SYNAPSE_CONFIG] [-p PROJECT_CONFIG] [-m MESSAGE] [-d] {primary}

Update data tables on Synapse for BPC databases

positional arguments:
  {primary}             Specify table type to run

optional arguments:
  -h, --help            show this help message and exit
  -s SYNAPSE_CONFIG, --synapse_config SYNAPSE_CONFIG
                        Synapse credentials file
  -p PROJECT_CONFIG, --project_config PROJECT_CONFIG
                        Project config file
  -m MESSAGE, --message MESSAGE
                        Version comment
  -d, --dry_run         dry run flag
```

Example run: 
```
python synthetic_update_data_table.py -p config.json -m 'synthetic data table update' primary
```
