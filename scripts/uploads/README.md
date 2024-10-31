# Merge and Uncode REDCap exports
[![automated](https://img.shields.io/docker/cloud/automated/sagebionetworks/genie-bpc-pipeline-uploads)](https://hub.docker.com/r/sagebionetworks/genie-bpc-pipeline-uploads)
![status](https://img.shields.io/docker/cloud/build/sagebionetworks/genie-bpc-pipeline-uploads)

## Installation

Clone this repository and navigate to the directory:
```
git clone git@github.com:Sage-Bionetworks/genie-bpc-pipeline.git
cd genie-bpc-pipeline/bpc/uploads/
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

## Usage: merging and uncoding REDCap Academic exports

To display the command line interface:
```
Rscript merge_and_uncode_rca_uploads.R -h
```

The command line interface will display as follows:
```
Usage: merge_and_uncode_rca_uploads.R [options]


Options:
        -c COHORT, --cohort=COHORT
                BPC cohort
        
        -u, --save_synapse
                Save output to Synapse

        -a SYNAPSE_AUTH, --synapse_auth=SYNAPSE_AUTH
                Path to .synapseConfig file or Synapse PAT (default: '~/.synapseConfig')

        -h, --help
                Show this help message and exit

        --production
                Whether to run in production mode or not (staging mode).
```

Example run (staging run):
```
Rscript merge_and_uncode_rca_uploads.R -c NSCLC -u -a $SYNAPSE_AUTH_TOKEN
```

Example run (production run):
```
Rscript merge_and_uncode_rca_uploads.R -c NSCLC -u -a $SYNAPSE_AUTH_TOKEN --production
```

## Usage: remove patient IDs from a REDCap formatted file

To display the command line interface:
```
Rscript remove_patients_from_merged.R -h
```

The command line interface will display as follows:
```
Usage: remove_patients_from_merged.R [options]


Options:
        -c COHORT, --cohort=COHORT
                BPC cohort code of patients to remove (required)

        -i SYNID_FOLDER_INPUT, --synid_folder_input=SYNID_FOLDER_INPUT
                Synapse ID of folder with merged and uncoded redcap export data (default: syn23286928)

        -o SYNID_FOLDER_OUTPUT, --synid_folder_output=SYNID_FOLDER_OUTPUT
                Synapse ID of output folder for filtered data (default: syn23286928).  Use 'NA' to write locally instead.

        -r SYNID_TABLE_RM, --synid_table_rm=SYNID_TABLE_RM
                Synapse ID of table with patient IDs to remove (default: syn29266682)

        -v, --verbose
                Output script messages to the user (default: FALSE)

        -a AUTH, --auth=AUTH
                Synapse personal access token or path to .synapseConfig (default: normal synapse login behavior)

        -h, --help
                Show this help message and exit
```

Example run: 
```
Rscript remove_patients_from_merged.R -i syn23285494 -c NSCLC -r syn29266682 -v
```

## Running tests
There are unit tests under `scripts/uploads/tests`.

1. Please pull and run the docker image associated with this module from [here](https://github.com/Sage-Bionetworks/genie-bpc-pipeline/pkgs/container/genie-bpc-pipeline) into your EC2/local.

You can also pull the production docker:

```bash
git pull sagebionetworks/genie-bpc-pipeline-uploads
```

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
R -e "remotes::install_cran('testthat')"
```

5. Run the following in a R session:

```R
library(testthat)
test_dir("/usr/local/src/myscripts/tests")
```
