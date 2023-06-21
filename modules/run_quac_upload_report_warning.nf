/*
Run quality asssurance checklist for the upload report at warning level.  
*/
process run_quac_upload_report_warning {

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
      Rscript genie-bpc-quac.R -c $cohort -s all -r upload -l warning -u -v
      """
   } 
   else {
      """
      cd /usr/local/src/myscripts/
      Rscript genie-bpc-quac.R -c $cohort -s all -r upload -l warning -v
      """
   }
}