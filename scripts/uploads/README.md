# Merge and Uncode REDCap exports



## Installation

Clone this repository and navigate to the directory:
```
git clone git@github.com:Sage-Bionetworks/Genie_processing.git
cd Genie_processing/bpc/uploads/
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

## Usage

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
```

Example run: 
```
Rscript merge_and_uncode_rca_uploads.R -c NSCLC -u -a $SYNAPSE_AUTH_TOKEN
```