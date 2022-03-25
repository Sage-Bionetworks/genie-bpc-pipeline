# GENIE BPC Pipeline

## Overview

This NextFlow workflow runs a preliminary version of the BPC pipeline formatted for use on a synthetic representation of the BPC dataset.  

Workflow proceses
1. Extract SYNAPSE_AUTH_TOKEN from .synapseConfig file.  This is necessary because the second step is a containerized R script that requires Synapse personal access token to be passes as a parameter or an environmental variable.
2. Run quality assurance checks.  This containerized R script returns an exit code corresponding to the number of issues detected.  If the number of issues is 0, the workflow continues.  If the number of issues is greater than 0, the workflow stops with an error. 
3. Uncode the synthetic REDCap dataset corresponding to the cohort specified in `nextflow.config` file.
4. Update Synapse tables with the uncoded synthetic data.  

## Installation

1. Clone this repository and navigate to the directory:
```
git clone git@github.com:Sage-Bionetworks/genie-bpc-pipeline.git
cd genie-bpc-pipeline
```

2. Install Nextflow.  Instructions are available at the following link: 

- https://www.nextflow.io/docs/latest/getstarted.html

## Synapse credentials

Cache your Synapse credentials in `bin/.synapseConfig` with the following format:
```
[authentication]

# cache authtoken below
authtoken = {your_personal_access_token_here}
```

## Usage

To run the Nextflow pipeline:
```
nextflow run main.nf
```
