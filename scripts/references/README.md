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
	-a AUTH, --auth=AUTH
		path to .synapseConfig or Synapse PAT (default: standard login precedence)

	-d , --dry_run
		Whether to dry-run or not.

	--production
		Whether to run in production mode (uses production project) or not (runs in staging mode and uses staging project).

	-h, --help
		Show this help message and exit

	-c, --comment
		Comment for new table snapshot version. This must be unique and is tied to the cohort run.
```

Example run (runs in staging mode) with version comment 3.0.1 for
potential PHI fields table when updated:
```
Rscript update_potential_phi_fields_table.R -c "version3.0.1"
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
Rscript update_cbio_mapping.R -v
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

## Running tests
There are unit tests under `scripts/references/tests`.

1. Please pull and run the docker image associated with this modules from [here](https://github.com/Sage-Bionetworks/genie-bpc-pipeline/pkgs/container/genie-bpc-pipeline) into your EC2/local.

```bash
docker run -d --name <nickname_for_container> <container_name> /bin/bash -c "while true; do sleep 1; done"
```

2. Do anything you need to do to the container (e.g: copy current local changes)

```bash
docker cp ./. test_container:/usr/local/src/myscripts
```

3. Execute container into a bash session

```bash
docker exec -it <nickname_for_container> /bin/bash
```

4. Install the `mockery` and `testthat` packages:

```bash
R -e "remotes::install_cran('mockery')"
R -e "remotes::install_cran('testthat')"
```

5. Run the following in a R session:

```R
library(testthat)
test_dir("/usr/local/src/myscripts/tests")
```
