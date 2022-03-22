# BPC Case Selection

## Installation

Clone this repository and navigate to the directory:
```
git clone git@github.com:Sage-Bionetworks/Genie_processing.git
cd Genie_processing/bpc/case_selection
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

## Usage: case selection

To display the command line interface:
```
Rscript workflow_case_selection.R -h
```

The command line interface will display as follows:
```
Usage: workflow_case_selection.R [options]


Options:
	-p PHASE, --phase=PHASE
		BPC phase

	-c COHORT, --cohort=COHORT
		BPC cohort

	-s SITE, --site=SITE
		BPC site

	-u, --save_synapse
		Save output to Synapse

	-h, --help
		Show this help message and exit
```

Example run: 
```
Rscript workflow_case_selection.R  -p 1 -c NSCLC -s DFCI -u
```

## Usage: create GENIE export files

To display the command line interface:
```
Rscript export_bpc_selected_cases.R -h
```

The command line interface will display as follows:
```
Usage: export_bpc_selected_cases.R [options]


Options:
	-i INPUT, --input=INPUT
		Synapse ID of the input file that has the BPC selected cases

	-o OUTPUT, --output=OUTPUT
		Synapse ID of the BPC output folder. Default: syn20798271

	-p PHASE, --phase=PHASE
		BPC phase. i.e. pilot, phase 1, phase 1 additional

	-c COHORT, --cohort=COHORT
		BPC cohort. i.e. NSCLC, CRC, BrCa, and etc.

	-s SITE, --site=SITE
		BPC site. i.e. DFCI, MSK, UHN, VICC, and etc.

	-h, --help
		Show this help message and exit
```

## Usage: update table counts

To display the command line interface:
```
Rscript update_case_count_table.R -h
```

The command line interface will display as follows:
```
Usage: update_case_count_table.R [options]


Options:
	-s, --save_synapse
		Save updated counts on Synapse

	-c COMMENT, --comment=COMMENT
		Comment for new table snapshot version

	-a SYNAPSE_AUTH, --synapse_auth=SYNAPSE_AUTH
		Path to .synapseConfig file or Synapse PAT (default: '~/.synapseConfig')

	-h, --help
		Show this help message and exit
```

Example run: 
```
Rscript update_case_count_table.R -s -c 'nsclc phase 2 update' -a $SYNAPSE_AUTH_TOKEN
```
