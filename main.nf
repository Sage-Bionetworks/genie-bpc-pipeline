#!/usr/bin/env nextflow

synapse_config1 = Channel.fromPath( 'bin/.synapseConfig' )
synapse_config2 = Channel.fromPath( 'bin/.synapseConfig' )
synapse_config3 = Channel.fromPath( 'bin/.synapseConfig' )

r_config = Channel.fromPath( 'bin/config.yaml' )
python_config = Channel.fromPath( 'bin/config.json' )

process getSynapseAuthToken {

   input:
   file file_synapse_config from synapse_config3

   output: 
   stdout into SYNAPSE_AUTH_TOKEN

   script:
   """
   grep 'authtoken = ' ${file_synapse_config} | cut -f 3 -d ' '
   """
}

process quacReports {

   input:
   val SYNAPSE_AUTH_TOKEN

   output:
   stdout into out

   script:
   """
   #docker run --rm hhunterzinck/genie-bpc-quac -c $cohort -s SAGE -r upload -l error -v -a $SYNAPSE_AUTH_TOKEN
   docker run --rm hhunterzinck/genie-bpc-quac -c $cohort -s SAGE -r masking -l error -v -a $SYNAPSE_AUTH_TOKEN
   """
}

out.view()

process uncodeSynthData {

   container 'synth-r'

   input:
   file file_synapse_config from synapse_config1
   file file_r_config from r_config

   script:

   if ( cohort == 'BLADDER')
        """
        synthetic_merge_and_uncode_rca.R -f syn27541023 -d syn26469280 -s syn26469947 -o 'bladder' -b -v -c ${file_r_config} -a ${file_synapse_config}
        """
   else if ( cohort == "BrCa") 
        """
        synthetic_merge_and_uncode_rca.R -f syn27541444 -d syn22738744 -s syn26469947 -o 'brca' -b -v -c ${file_r_config} -a ${file_synapse_config}
        """
   else if (cohort == "CRC") 
        """
        synthetic_merge_and_uncode_rca.R -f syn27541444 -d syn22738744 -s syn26469947 -o 'crc' -b -v -c ${file_r_config} -a ${file_synapse_config}
        """
   else if (cohort == "NSCLC")
        """
        synthetic_merge_and_uncode_rca.R -f syn27542392 -d syn25610053 -s syn26469947 -o 'nsclc' -b -v -c ${file_r_config} -a ${file_synapse_config}
        """
   else if (cohort == "PANC") 
        """
        synthetic_merge_and_uncode_rca.R -f syn27538210 -d syn25468849 -s syn26469947 -o 'panc' -b -v -c ${file_r_config} -a ${file_synapse_config}
        """
   else if (cohort == "Prostate") 
        """
        synthetic_merge_and_uncode_rca.R -f syn27542446 -d syn26260844 -s syn26469947 -o 'prostate' -b -v -c ${file_r_config} -a ${file_synapse_config}
        """
   else
        error "Invalid alignment mode: ${cohort}"
}

process updateSynapseTables {

   container 'synth-python'

   input:
   file file_synapse_config from synapse_config2
   file file_python_config from python_config

   script:
   """
   synthetic_update_data_table.py -p ${file_python_config} -s ${file_synapse_config} -m 'synthetic data table update from nextflow' primary
   """
}

