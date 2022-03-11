# Description: create synthetic site datasets and merge
# Author: Haley Hunter-Zinck
# Date: 2022-01-10

# uploads ------------------

# generate synthetic, coded data for test cohort
Rscript generate_synthetic_redcap_data.R  -d syn26469280 -c 'BPC Bladder Cancer' -n 3 -s SAGE -v -p GENIE
Rscript generate_synthetic_redcap_data.R  -d syn24181706 -c 'BPC Breast Cancer' -n 3 -s SAGE -v -p GENIE
Rscript generate_synthetic_redcap_data.R  -d syn22738744 -c 'BPC Colon/Rectum Cancer' -n 3 -s SAGE -v -p GENIE
Rscript generate_synthetic_redcap_data.R  -d syn25610053 -c 'BPC NSCLC Cancer' -n 3 -s SAGE -v -p GENIE
Rscript generate_synthetic_redcap_data.R  -d syn25468849 -c 'BPC Pancreas Cancer' -n 3 -s SAGE -v -p GENIE
Rscript generate_synthetic_redcap_data.R  -d syn26260844 -c 'BPC Prostate Cancer' -n 3 -s SAGE -v -p GENIE

# merged -------------------

# merge and uncode data for all cohorts for site SAGE
Rscript merge_and_uncode_rca_synthetic.R -f syn27541023 -d syn26469280 -s syn26469947 -o 'bladder' -b -v
Rscript merge_and_uncode_rca_synthetic.R -f syn27541333 -d syn24181706 -s syn26469947 -o 'brca' -b -v
Rscript merge_and_uncode_rca_synthetic.R -f syn27541444 -d syn22738744 -s syn26469947 -o 'crc' -b -v
Rscript merge_and_uncode_rca_synthetic.R -f syn27542392 -d syn25610053 -s syn26469947 -o 'nsclc' -b -v
Rscript merge_and_uncode_rca_synthetic.R -f syn27538210 -d syn25468849 -s syn26469947 -o 'panc' -b -v
Rscript merge_and_uncode_rca_synthetic.R -f syn27542446 -d syn26260844 -s syn26469947 -o 'prostate' -b -v

# tables -------------------

python update_data_table.py -p config.json -m 'synthetic data table update' primary
