# BPC Case Selection

## Installation

Clone this repository and navigate to the directory:
```
git clone git@github.com:Sage-Bionetworks/genie-bpc-pipeline.git
cd genie-bpc-pipeline/scripts/case_selection
```

Install all required Python package and quarto cli.
Install pandoc: https://github.com/jgm/pandoc/releases/tag/3.1.11.1

```
pip install aacrgenie jupyter
```

## Synapse credentials

Cache your Synapse personal access token (PAT) as an environmental variable:
```
export SYNAPSE_AUTH_TOKEN={your_personal_access_token_here}
```

## Usage: case selection

* Performing case selection

    ```
    python perform_case_selection.py -p 1 -c NSCLC -s DFCI   
    ```

* Rendering quarto case selection

    ```
    quarto render case_selection.qmd -P phase:1 -P cohort:NSCLC -P site:DFCI
    ```

* workflow case selection

	```
	python workflow_case_selection.py -p 1 -c NSCLC -s DFCI
	```

## Usage: create GENIE export files

To display the command line interface:
```
python export_bpc_selected_cases.py -i syn31068082 -p 2 -c CRC -s DFCI
```


## Usage: update table counts

```
python update_case_count_table.py -h
usage: update_case_count_table.py [-h] [-s] [-c COMMENT] [-a SYNAPSE_AUTH]

optional arguments:
  -h, --help            show this help message and exit
  -s, --save_synapse    Save updated counts on Synapse
  -c COMMENT, --comment COMMENT
                        Comment for new table snapshot version

```

Example run: 
```
python update_case_count_table.py -s -c 'nsclc phase 2 update'
```


## Usage: update table with case selection criteria

To display the command line interface:
```
python update_case_criteria_table.py -h
```

Example run: 
```
python update_case_criteria_table.py -s  -c 'example run' -v
```

## Configuration

### Case selection for a new cohort
To conduct case selection for a new cohort, modify the `config.yaml` file following the template from another cohort.  Case selection for a novel cohort requires the cohort phase, name, production targets for each site, max and min date of sequencing, and a list of eligible OncoTree codes.  The root OncoTree code is just for reporting and chosen by Sage (usually just a parent code of eligible OncoTree codes).  Site-specific sequencing dates or SDV and IRR targets can be specified underneath each site key in the respective cohort.  The `adjusted` key under each site may be set to the production target on initialization.  

### Adjusted target numbers
During curation, sites may find cases to be ineligible and may have no additional case for which to substitute the ineligible case.  To account for expected reductions in the target count, update the `adjusted` target number in the `config.yaml` file.  These numbers will then be reflected in the `Case Selection Counts` Table on Synapse (syn26228746) and used in the QA checks to verify the number of submitted cases from each site matches the adjusted target.  

### Selection of additional samples
After initial case selection and curation, additional samples for selected cases may be submitted through the main GENIE releases.  These additional samples are collected for curators upon request.  These cohorts are defined in much the same way as other cohorts but without production targets.  See format under the `1_additional` key in the `config.yaml` file for an example.  