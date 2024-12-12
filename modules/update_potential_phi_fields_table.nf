/*
Updates the potential pHI fields table with any new variables to redact
*/
process update_potential_phi_fields_table {

   container "$params.references_docker"
   secret 'SYNAPSE_AUTH_TOKEN'
   debug true

   input:
   val comment
   val production

   output:
   stdout

   script:
   if (production) {
      """
      cd /usr/local/src/myscripts/
      Rscript update_potential_phi_fields_table.R -c $comment --production
      """
   } 
   else {
      """
      cd /usr/local/src/myscripts/
      Rscript update_potential_phi_fields_table.R -c $comment
      """
   } 
}
