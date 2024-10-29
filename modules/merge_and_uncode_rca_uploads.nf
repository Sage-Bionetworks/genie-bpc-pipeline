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

   output:
   stdout

   script:
   if (production) {
      """
      cd /usr/local/src/myscripts/
      Rscript merge_and_uncode_rca_uploads.R -c $cohort -v --production --save_synapse -cmt $comment
      """
   }
   else {
      """
      cd /usr/local/src/myscripts/
      Rscript merge_and_uncode_rca_uploads.R -c $cohort -v --save_synapse -cmt $comment
      """
   }
}