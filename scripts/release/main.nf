/*
Run the clinical release
*/
process run_clinical_release {

   container 'sagebionetworks/genie-bpc-pipeline-clinical-release'
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
      Rscript create_release_files.R -c $cohort -v -s
      """
   }
   else {
      """
      cd /usr/local/src/myscripts/
      Rscript create_release_files.R -c $cohort -v
      """
   }
}

params.cohort = 'NSCLC'
/* 
Note: For multi-word strings like in the param comment here, everywhere that calls $comment as an argument
needed to be enclosed with double quotes so that nextflow interprets it as an entire string and 
not separate command line arguments 
*/
params.comment = 'NSCLC public release update'
params.production = false
params.schema_ignore_params = ""
params.help = false

run_clinical_release(previous: '', cohort: params.cohort, production: params.production)