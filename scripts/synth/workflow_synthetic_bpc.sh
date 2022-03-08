# Description: create synthetic site datasets and merge
# Author: Haley Hunter-Zinck
# Date: 2022-01-10

# PROSTATE ------------

# generate synthetic, coded data for three sites
Rscript generate_synthetic_redcap_data.R  -d syn26260844 -c 'BPC Prostate Cancer' -n 3 -s synthA -v -u syn26469946
Rscript generate_synthetic_redcap_data.R  -d syn26260844 -c 'BPC Prostate Cancer' -n 5 -s synthB -v -u syn26469946
Rscript generate_synthetic_redcap_data.R  -d syn26260844 -c 'BPC Prostate Cancer' -n 7 -s synthC -v -u syn26469946

# PANC ------------------

# generate synthetic, coded data for three sites
Rscript generate_synthetic_redcap_data.R  -d syn25468849 -c 'BPC Pancreas Cancer' -n 2 -s synthA -v -u syn26469946
Rscript generate_synthetic_redcap_data.R  -d syn25468849 -c 'BPC Pancreas Cancer' -n 4 -s synthB -v -u syn26469946
Rscript generate_synthetic_redcap_data.R  -d syn25468849 -c 'BPC Pancreas Cancer' -n 6 -s synthC -v -u syn26469946

# COHORT ------------------

# generate synthetic, coded data for test cohort
Rscript generate_synthetic_redcap_data.R  -d syn26469280 -c 'BPC Bladder Cancer' -n 3 -s SAGE -v -p GENIE
Rscript generate_synthetic_redcap_data.R  -d syn24181706 -c 'BPC Breast Cancer' -n 3 -s SAGE -v -p GENIE
Rscript generate_synthetic_redcap_data.R  -d syn22738744 -c 'BPC Colon/Rectum Cancer' -n 3 -s SAGE -v -p GENIE
Rscript generate_synthetic_redcap_data.R  -d syn25610053 -c 'BPC NSCLC Cancer' -n 3 -s SAGE -v -p GENIE
Rscript generate_synthetic_redcap_data.R  -d syn25468849 -c 'BPC Pancreas Cancer' -n 3 -s SAGE -v -p GENIE
Rscript generate_synthetic_redcap_data.R  -d syn26260844 -c 'BPC Prostate Cancer' -n 3 -s SAGE -v -p GENIE

# merge -------------------

# merge and uncode data from three sites
Rscript merge_and_uncode_rca_synthetic.R -f syn26998567,syn26469958,syn26469959 -d syn26260844 -s syn26469947 -o 'prostate' -b -v
Rscript merge_and_uncode_rca_synthetic.R -f syn27000200,syn27000206,syn27000235 -d syn25468849 -s syn26469947 -o 'panc' -b -v
