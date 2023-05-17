# BPC drug masking reports

## Installation

Clone this repository and navigate to the directory:
```
git clone git@github.com:Sage-Bionetworks/genie-bpc-pipeline.git
cd genie-bpc-pipeline/bpc/masking
```

Install all required R packages:
```
R -e 'renv::restore()'
```

## Downloading HemOnc reference files

Instructions on accessing HemOnc Ontology reference files are available here: https://hemonc.org/wiki/Ontology#Availability

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

## Usage: update HemOnc reference tables

To display the command line interface:
```
Rscript update_hemonc_tables.R -h
```

The command line interface will display as follows:
```
Usage: update_hemonc_tables.R [options]


Options:
	-c CONCEPT_FILE, --concept_file=CONCEPT_FILE
		Path to file containing HemOnc concepts

	-r RELATIONSHIP_FILE, --relationship_file=RELATIONSHIP_FILE
		Path to file containing HemOnc relationships

	-m COMMENT, --comment=COMMENT
		Comment for new snapshot version

	-h, --help
		Show this help message and exit
```

Example run: 
```
Rscript update_hemonc_tables.R -c 2022-01-05.ccby_concepts.csv -r 2022-01-05.ccby_rels.csv -m '2022-01-05 update'
```

## Usage: generate drug masking reports

To display the command line interface:
```
Rscript workflow_unmasked_drugs.R -h
```

The command line interface will display as follows:
```
Usage: workflow_unmasked_drugs.R [options]


Options:
	-c COHORT, --cohort=COHORT
		Cohort on which run analysis

	-d DATE, --date=DATE
		Upload date for folder labels

	-s, --save_synapse
		Save output to Synapse

	-a SYNAPSE_AUTH, --synapse_auth=SYNAPSE_AUTH
		Path to .synapseConfig file or Synapse PAT (default: '~/.synapseConfig')

	-h, --help
		Show this help message and exit
```

Example run: 
```
Rscript workflow_unmasked_drugs.R -c NSCLC -d 2022-02-01 -s -a $SYNAPSE_AUTH_TOKEN
```
