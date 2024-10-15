/*

*/
process update_potential_phi_fields {

   container 'sagebionetworks/genie-bpc-pipeline-references'
   secret 'SYNAPSE_AUTH_TOKEN'
   debug true

   input:
   val previous
   val comment
   val production

   output:
   stdout

   script:
    if (production) {
        """
        cd /usr/local/src/myscripts/
        Rscript update_potential_phi_fields.R -c $comment --production
        """
    } else {
        """
        cd /usr/local/src/myscripts/
        Rscript update_potential_phi_fields.R -c $comment
        """
    } 
}