# GENIE BPC data release scripts

## Overview
These scripts pertain to the final steps for generating BPC data releases.  

## Installation

Clone this repository and navigate to the directory:
```
git clone git@github.com:Sage-Bionetworks/genie-bpc-pipeline.git
cd genie-bpc-pipeline/scripts/release
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

## Usage: clinical file generation

To display the command line interface:
```
Rscript create_release_files.R -h
```

The command line interface will display as follows:
```
Usage: create_release_files.R [options]


Options:
        -c COHORT, --cohort=COHORT
                BPC cohort. i.e. NSCLC, CRC, BrCa, and etc.

        -s, --save_to_synapse
                Save files to Synapse and delete local copies

        -a AUTH, --auth=AUTH
                Synapse personal access token or path to .synapseConfig (default: normal synapse login behavior)

        -v, --verbose
                Print script progress to the user

        -h, --help
                Show this help message and exit
```

Example run: 
```
Rscript create_release_files.R -c BLADDER -v
```
