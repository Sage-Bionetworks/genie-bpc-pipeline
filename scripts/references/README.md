# Reference updates

## Overview

These scripts pertain to updating references used in BPC data processing.

## Installation

Clone this repository and navigate to the directory:
```
git clone git@github.com:Sage-Bionetworks/genie-bpc-pipeline.git
cd genie-bpc-pipeline/scripts/references
```

Install all required R packages:
```
R -e 'renv::restore()'
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

## Usage: updating potential PHI fields to redact 

To display the command line interface:
```
Rscript update_potential_phi_fields_table.R -h
```

The command line interface will display as follows:
```
Usage: update_potential_phi_fields_table.R [options]


Options:
	-f SYNID_FILE_SOR, --synid_file_sor=SYNID_FILE_SOR
		Synapse ID of Scope of Release file (default: syn22294851)

	-t SYNID_TABLE_RED, --synid_table_red=SYNID_TABLE_RED
		Synapse ID of table listing variables to redact (default: syn23281483)

	-a AUTH, --auth=AUTH
		path to .synapseConfig or Synapse PAT (default: standard login precedence)

	-h, --help
		Show this help message and exit
```

Example run: 
```
Rscript update_potential_phi_fields_table.R 
```

## Usage: updating the cBioPortal mapping table 

To display the command line interface:
```
Rscript update_cbio_mapping.R -h
```

The command line interface will display as follows:
```
Usage: update_cbio_mapping.R [options]


Options:
        -s, --save_to_synapse
                Save mapping to Synapse table and delete local output file

        -c COMMENT, --comment=COMMENT
                Comment for table snapshot if saving to synapse (optional)

        -a AUTH, --auth=AUTH
                Synapse personal access token or path to .synapseConfig (default: normal synapse login behavior)

        -v, --verbose
                Print script progress to the user

        -h, --help
                Show this help message and exit
```

Example run: 
```
Rscript update_potential_phi_fields_table.R -v
```

## Usage: updating upload tracking table 

To display the command line interface:
```
Rscript update_date_tracking_table.R -h
```

The command line interface will display as follows:
```
Usage: update_date_tracking_table.R [options]


Options:
	-c COHORT, --cohort=COHORT
		BPC cohort

	-d DATE, --date=DATE
		New current date for cohort

	-s SAVE_COMMENT, --save_comment=SAVE_COMMENT
		Save table snapshot to Synapse with supplied comment

	-h, --help
		Show this help message and exit
```

Example run: 
```
Rscript update_date_tracking_table.R -c CRC -d 2022-03-31 -s 'round x update to crc'
```
