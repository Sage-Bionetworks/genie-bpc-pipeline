/*
Merge and uncode REDcap export data files.   
*/
process merge_and_uncode_rca_uploads {

   container 'sagebionetworks/genie-bpc-pipeline-uploads'
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
      Rscript merge_and_uncode_rca_uploads.R -c $cohort -u -v
      """
   }
   else {
      """
      cd /usr/local/src/myscripts/
      Rscript merge_and_uncode_rca_uploads.R -c $cohort -v
      """
   }
}