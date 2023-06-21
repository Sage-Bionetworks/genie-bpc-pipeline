/*
Run quality asssurance checklist for the upload report at error level.  
Stop the workflow if any errors are detected.
*/
process run_quac_upload_report_error {

   container 'sagebionetworks/genie-bpc-quac'
   secret 'SYNAPSE_AUTH_TOKEN'
   debug true

   input:
   val cohort

   output:
   stdout

   script:
   """
   cd /usr/local/src/myscripts/
   Rscript genie-bpc-quac.R -c $cohort -s all -r upload -l error -v
   """
}