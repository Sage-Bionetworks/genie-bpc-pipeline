/*
Remove temporarily retracted (redacted) patients from merged upload file.
*/
process remove_patients_from_merged {

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
      Rscript remove_patients_from_merged.R -c $cohort -v
      """
   }
   else {
      """
      cd /usr/local/src/myscripts/
      Rscript remove_patients_from_merged.R -c $cohort -v -o NA
      """
   }
}