#!/usr/bin/env nextflow

params.cohort = 'NSCLC'
params.comment = 'NSCLC public release update'
params.synapse_config = '.synapseConfig'

ch_cohort = Channel.value(params.cohort)
ch_comment = Channel.value(params.comment)
ch_synapse_config = Channel.value(file(params.synapse_config))

/*
Check cohort code is one of the valid values.
*/
process checkCohortCode {
    input:
    val cohort from ch_cohort

    output:
    stdout into status

    script:
    """
    echo $cohorts | tr ' ' '\n' | grep -c ^$cohort\$
    """
}

/*
Run quality asssurance checklist for the upload report at error level.  
Stop the workflow if any errors are detected.
*/
process quacUploadReportError {

   container 'hhunterzinck/genie-bpc-quac'

   input:
   file syn_config   from ch_synapse_config
   val cohort        from ch_cohort

   output:
   stdout into outUploadReportError

   script:
   """
   R -e 'renv::restore(lockfile = "/usr/local/src/myscripts/renv.lock")'
   Rscript /usr/local/src/myscripts/genie-bpc-quac.R -c $cohort -s all -r upload -l error -v -a $syn_config 
   """
}

outUploadReportError.view()

/*
Run quality asssurance checklist for the upload report at warning level.  
Do not stop the workflow if any issues are detected.
*/
process quacUploadReportWarning {

   container 'hhunterzinck/genie-bpc-quac'
   errorStrategy 'ignore'

   input:
   file syn_config   from ch_synapse_config
   val cohort        from ch_cohort

   output:
   stdout into outUploadReportWarning

   script:
   """
   R -e 'renv::restore(lockfile = "/usr/local/src/myscripts/renv.lock")'
   Rscript /usr/local/src/myscripts/genie-bpc-quac.R -c $cohort -s all -r upload -l warning -u -v -a $syn_config 
   """
}

outUploadReportWarning.view()

/*
Merge and uncode REDcap export data files.   
*/
process mergeAndUncodeRcaUploads {

   container 'hhunterzinck/genie-bpc-pipeline-uploads'

   input:
   file syn_config   from ch_synapse_config
   val cohort        from ch_cohort

   output:
   stdout into outMergeAndUncodeRcaUploads

   script:
   """
   R -e 'renv::restore(lockfile = "/usr/local/src/myscripts/renv.lock")'
   Rscript /usr/local/src/myscripts/merge_and_uncode_rca_uploads.R -c $cohort -u -a $syn_config -v
   """
}

outMergeAndUncodeRcaUploads.view()

/*
Update Synapse tables with merged and uncoded data.
*/
process updateDataTable {

   container 'hhunterzinck/genie-bpc-pipeline-table-updates'

   input:
   file syn_config   from ch_synapse_config
   val comment       from ch_comment

   script:
   """
   pip install -r /root/scripts/requirements.txt
   python /root/scripts/update_data_table.py -s $syn_config -p /root/scripts/config.json -m $comment primary
   """
}

/*
Update reference table storing the date of current and previous Synapse table updates
for later quality assurance checklist reports.s
*/
process updateDateTrackingTable {

   container 'hhunterzinck/genie-bpc-pipeline-references'

   input:
   file syn_config   from ch_synapse_config
   val cohort        from ch_cohort
   val comment       from ch_comment

   script:
   """
   R -e 'renv::restore(lockfile = "/usr/local/src/myscripts/renv.lock")'
   Rscript /usr/local/src/myscripts/update_date_tracking_table.R -c $cohort -d `date +'%Y-%m-%d'` -s $comment -a $syn_config 
   """
}

/*
Run quality asssurance checklist for the table report at error and warning level.  
Do not stop the workflow if any issues are detected.
*/
process quacTableReport {

   container 'hhunterzinck/genie-bpc-quac'
   errorStrategy 'ignore'

   input:
   file syn_config   from ch_synapse_config
   val cohort        from ch_cohort

   output:
   stdout into outTableReport

   script:
   """
   R -e 'renv::restore(lockfile = "/usr/local/src/myscripts/renv.lock")'
   Rscript /usr/local/src/myscripts/genie-bpc-quac.R -c $cohort -s all -r table -l error -u -v -a $syn_config 
   Rscript /usr/local/src/myscripts/genie-bpc-quac.R -c $cohort -s all -r table -l warning -u -v -a $syn_config 
   """
}

outTableReport.view()

/*
Run quality asssurance checklist for the comparison report at error and warning level.  
Do not stop the workflow if any issues are detected.
*/
process quacComparisonReport {

   container 'hhunterzinck/genie-bpc-quac'
   errorStrategy 'ignore'

   input:
   file syn_config   from ch_synapse_config
   val cohort        from ch_cohort

   output:
   stdout into outComparisonReport

   script:
   """
   R -e 'renv::restore(lockfile = "/usr/local/src/myscripts/renv.lock")'
   Rscript /usr/local/src/myscripts/genie-bpc-quac.R -c $cohort -s all -r comparison -l error -u -v -a $syn_config 
   Rscript /usr/local/src/myscripts/genie-bpc-quac.R -c $cohort -s all -r comparison -l warning -u -v -a $syn_config 
   """
}

outComparisonReport.view()

/*
Create drug masking report files on most recent Synapse table data.  
*/
process maskingReport {

   container 'hhunterzinck/genie-bpc-pipeline-masking'

   input:
   file syn_config   from ch_synapse_config
   val cohort        from ch_cohort

   output:
   stdout into outMaskingReport

   script:
   """
   R -e 'renv::restore(lockfile = "/usr/local/src/myscripts/renv.lock")'
   Rscript /usr/local/src/myscripts/workflow_unmasked_drugs.R -c $cohort -d `date +'%Y-%m-%d'` -s -a $syn_config 
   """
}

outMaskingReport.view()

/*
Update the case count table with current case counts calculated
from Synapse tables. 
*/
process updateCaseCountTable {

   container 'hhunterzinck/genie-bpc-pipeline-case-selection'

   input:
   file syn_config   from ch_synapse_config
   val comment       from ch_comment

   output:
   stdout into outCaseCount

   script:
   """
   R -e 'renv::restore(lockfile = "/usr/local/src/myscripts/renv.lock")'
   Rscript /usr/local/src/myscripts/update_case_count_table.R -s -c $comment -a $syn_config
   """
}

outCaseCount.view()

