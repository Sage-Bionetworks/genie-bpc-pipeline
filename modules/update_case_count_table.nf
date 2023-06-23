/*
Update the case count table with current case counts calculated
from Synapse tables. 
*/
process update_case_count_table {

   container 'sagebionetworks/genie-bpc-pipeline-case-selection'
   secret 'SYNAPSE_AUTH_TOKEN'
   debug true

   input:
   val previous
   val comment
   val production

   output:
   stdout

   script:
   if (production) {
      """
      cd /usr/local/src/myscripts/
      Rscript update_case_count_table.R -s -c "$comment"
      """
   }
   else {
      """
      cd /usr/local/src/myscripts/
      Rscript update_case_count_table.R
      """
   }
}
