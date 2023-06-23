/*
Run quality asssurance checklist for the comparison report at error and warning level.  
*/
process run_quac_comparison_report {

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
      Rscript genie-bpc-quac.R -c $cohort -s all -r comparison -l error -u -v
      Rscript genie-bpc-quac.R -c $cohort -s all -r comparison -l warning -u -v
      """
   }
   else {
      """
      cd /usr/local/src/myscripts/
      Rscript genie-bpc-quac.R -c $cohort -s all -r comparison -l error -v
      Rscript genie-bpc-quac.R -c $cohort -s all -r comparison -l warning -v
      """
   }
}
