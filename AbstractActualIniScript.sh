#!/bin/bash

hadoop fs -rm -r hdfs://ssehdp101.metmom.mmih.biz:8020/Ayolanding/AbstractActualTabIncrementalOut;
sqoop job --delete abstractActualImportJob;
echo "    #####    Sqoop job creation    ######"

sqoop job --create abstractActualImportJob -- import --options-file '/home/hdfs/sqoopimport/connectionDetails.txt' --password-file 'hdfs://ssehdp101.metmom.mmih.biz:8020/passwd/psw.txt' --table IAASPF.TBL_ABSTRACT_ACTUAL --append --incremental lastmodified --check-column CHANGED_AT --fields-terminated-by '\t' --target-dir hdfs://ssehdp101.metmom.mmih.biz:8020/Ayolanding/AbstractActualTabIncrementalOut -m 1;
if [ $? -eq 0 ]; then
echo "***************************************************************"
echo "      Successfully Sqoop Job is created     "
echo "***************************************************************"
else
    echo "Sqoop job creation failed"
fi
echo " #### starting Sqoop Impport  ####"

sqoop job --exec abstractActualImportJob;

if [ $? -eq 0 ]; then
echo "***************************************************************"
echo "      Successfully Sqoop Import is completed     "
echo "***************************************************************"
else
    echo "Sqoop Import is failed"
fi

hive -e " drop table ayolanding.temp_IAASPF_TBL_ABSTRACT_ACTUAL;"
hive -e " drop table ayolanding.IAASPF_TBL_ABSTRACT_ACTUAL;"

hive -e "create external table ayolanding.temp_IAASPF_TBL_ABSTRACT_ACTUAL(id bigint,type_discriminator string,version bigint,type_id bigint,uuid string,kind_id bigint,origin_request_id bigint,composite_comp_list_id bigint,index_comp_list int,lb_agreement_version_id bigint,role_in_actual_context_id bigint,only_actual_id bigint,changed_at string) row format delimited fields terminated by '\t' lines terminated by '\n' stored as textfile LOCATION 'hdfs://ssehdp101.metmom.mmih.biz:8020/Ayolanding/AbstractActualTabIncrementalOut';"
if [ $? -eq 0 ]; then
echo "***************************************************************"
echo "      Successfully External Table is created     "
echo "***************************************************************"
else
    echo "External Table creation failed"
fi

hive -e "create table ayolanding.IAASPF_TBL_ABSTRACT_ACTUAL(id bigint,type_discriminator string,version bigint,type_id bigint,uuid string,kind_id bigint,origin_request_id bigint,composite_comp_list_id bigint,index_comp_list int,lb_agreement_version_id bigint,role_in_actual_context_id bigint,only_actual_id bigint,changed_at string) row format delimited fields terminated by '\t' lines terminated by '\n' stored as orcfile;"
if [ $? -eq 0 ]; then

echo "***************************************************************"
echo "      Successfully ORC format Table is created     "
echo "***************************************************************"
else
    echo " ORC Table creation failed"
fi

echo " starting loading records into ORC format table "

hive -e "INSERT OVERWRITE TABLE ayolanding.IAASPF_TBL_ABSTRACT_ACTUAL select id,type_discriminator,version,type_id,uuid,kind_id,origin_request_id,composite_comp_list_id,index_comp_list,lb_agreement_version_id,role_in_actual_context_id,only_actual_id,changed_at from (select ROW_NUMBER() over(partition by id order by changed_at desc) rowno,* from ayolanding.temp_IAASPF_TBL_ABSTRACT_ACTUAL)a where rowno=1;"
if [ $? -eq 0 ]; then
echo "***************************************************************"
echo "      Successfully ORC Table loading is completed     "
echo "***************************************************************"
else
    echo "ORC Table loading is failed"
fi