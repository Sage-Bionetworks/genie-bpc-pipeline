/*
Merge and uncode REDcap export data files.   
*/
process merge_and_uncode_rca_uploads {

   container "$params.uploads_docker"
   secret 'SYNAPSE_AUTH_TOKEN'
   debug true

   input:
   val previous
   val cohort
   val comment
   val production
   val use_grs

   output:
   stdout

   script:
   if (production) {
      """
      cd /usr/local/src/myscripts/
      Rscript merge_and_uncode_rca_uploads.R \
         -c $cohort 
         -v \
         --production \
         --save_synapse \
         --comment $comment \
         --use_grs $use_grs
      """
   }
   else {
      """
      cd /usr/local/src/myscripts/
      Rscript merge_and_uncode_rca_uploads.R \
         -c $cohort \
         -v \
         --save_synapse \
         --comment $comment \
         --use_grs $use_grs
      """
   }
}