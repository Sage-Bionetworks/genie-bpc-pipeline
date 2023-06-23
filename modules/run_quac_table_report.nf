/*
Run quality asssurance checklist for the table report at error and warning level.  
Do not stop the workflow if any issues are detected.
*/
process run_quac_table_report {

   container 'sagebionetworks/genie-bpc-quac'
   secret 'SYNAPSE_AUTH_TOKEN'
   debug true

   input:
   val previous
   val cohort
   val production

   output:
   stdout

   script:
   if (production) {
      """
      cd /usr/local/src/myscripts/
      Rscript genie-bpc-quac.R -c $cohort -s all -r table -l error -u -v
      Rscript genie-bpc-quac.R -c $cohort -s all -r table -l warning -u -v
      """
   }
   else {
      """
      cd /usr/local/src/myscripts/
      Rscript genie-bpc-quac.R -c $cohort -s all -r table -l error -v
      Rscript genie-bpc-quac.R -c $cohort -s all -r table -l warning -v
      """
   }
}