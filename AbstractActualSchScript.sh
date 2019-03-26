TIMESTAMP=`date "+%Y-%m-%d"`
touch /home/$USER/logs/${TIMESTAMP}.success_log
touch /home/$USER/logs/${TIMESTAMP}.fail_log
success_logs=/home/$USER/logs/${TIMESTAMP}.success_log
failed_logs=/home/$USER/logs/${TIMESTAMP}.fail_log

#Function to get the status of the job creation
function log_status
{
       status=$1
       message=$2
       if [ "$status" -ne 0 ]; then
                echo "`date +\"%Y-%m-%d %H:%M:%S\"` [ERROR] $message [Status] $status : failed" | tee -a "${failed_logs}"
                mail -a /home/$USER/logging/"AbstractActlImport_log" -s "This is the failed job log" ravikirandasari@gmail.com < /home/$USER/logs/${TIMESTAMP}.fail_log
                #exit 1
                else
                    echo "`date +\"%Y-%m-%d %H:%M:%S\"` [INFO] $message [Status] $status : success" | tee -a "${success_logs}"
                  # mail  -a /home/$USER/logging/"AbstractActlImport_log" -s "This is the Success job log " ravikirandasari@gmail.com < /home/$USER/logs/${TIMESTAMP}.success_log
                fi
}

echo " #### starting Sqoop Impport  ####"

sqoop job --exec abstractActualImportJob > /home/$USER/logging/"AbstractActlImport_log" 2>&1
  g_STATUS=$?
  log_status $g_STATUS "IAASPF_TBL_ABSTRACT_ACTUAL table Import"

if [ $? -eq 0 ]; then
echo "***************************************************************"
echo "      Successfully Sqoop Import is completed     "
echo "***************************************************************"
else
    echo "Sqoop Import is failed"
fi

echo " starting loading records into ORC format table "

hive -e "INSERT OVERWRITE TABLE ayolanding.IAASPF_TBL_ABSTRACT_ACTUAL select id,type_discriminator,version,type_id,uuid,kind_id,origin_request_id,composite_comp_list_id,index_comp_list,lb_agreement_version_id,role_in_actual_context_id,only_actual_id,changed_at from (select ROW_NUMBER() over(partition by id order by changed_at desc) rowno,* from ayolanding.temp_IAASPF_TBL_ABSTRACT_ACTUAL)a where rowno=1;" >> /home/$USER/logging/"AbstractActlImport_log" 2>&1
  g_STATUS=$?
  log_status $g_STATUS "Over Writing IAASPF_TBL_ABSTRACT_ACTUAL"

#if [ $? -eq 0 ]; then
#mail -s "The AbstractActual Import is successfully Completed " barry.vorster@ayoholdings.com < /home/$USER/logs/${TIMESTAMP}.success_log
#echo "***************************************************************"
#echo "      Successfully ORC Table loading is completed     "
#echo "***************************************************************"
#else
#    echo "ORC Table loading is failed"
#fi

hive -e "INSERT OVERWRITE TABLE ayolanding.temp_iaaspf_tbl_abstract_actual select * from ayolanding.iaaspf_tbl_abstract_actual;" >> /home/$USER/logging/"AbstractActlImport_log" 2>&1
  g_STATUS=$?
  log_status $g_STATUS "Over Writing TEMP_IAASPF_TBL_ABSTRACT_ACTUAL"
