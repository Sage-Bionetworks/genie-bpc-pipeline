# REDCap Data dictionary processing

The describe below generate the non-PHI REDCap data dictionaries and import templates.  

## Installation

Clone this repository and navigate to the directory:
```
git clone git@github.com:Sage-Bionetworks/genie-bpc-pipeline.git
cd genie-bpc-pipeline/bpc/dd
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

## Usage: non-PHI data dictionary

To display the command line interface:
```
Rscript main_dd_to_nonphi_dd.R -h
```

The command line interface will display as follows:
```
Usage: main_dd_to_nonphi_dd.R [options]


Options:
	-d SYNID_FILE_DD, --synid_file_dd=SYNID_FILE_DD
		Synapse ID of data dictionary file

	-f SYNID_FOLDER_DD, --synid_folder_dd=SYNID_FOLDER_DD
		Synapse ID of folder to store non-PHI data dictionary

	-h, --help
		Show this help message and exit
```

Example run: 
```
Rscript main_dd_to_nonphi_dd.R -d syn26469277 -f syn26469274
```

## Usage: import template

To display the command line interface:
```
Rscript Rscript main_import_template.R -h
```

The command line interface will display as follows:
```
Usage: main_import_template.R [options]


Options:
	-d SYNID_FILE_DD, --synid_file_dd=SYNID_FILE_DD
		Synapse ID of non-PHI data dictionary file

	-f SYNID_FOLDER_OUTPUT, --synid_folder_output=SYNID_FOLDER_OUTPUT
		Synapse ID of folder to store generated import template

	-h, --help
		Show this help message and exit
```

Example run: 
```
Rscript main_import_template.R -d syn26469277 -f syn26469274
```
