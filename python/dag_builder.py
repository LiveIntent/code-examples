from airflow import DAG
from airflow import settings
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.exceptions import AirflowException
from airflow.macros.liveintent_plugin import sns_on_failure, sns_on_success, sns_on_late
from airflow.models import Variable
from airflow.operators import DummyOperator
from airflow.operators.liveintent_plugin import HadoopSSHOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from mapping_and_aggregation.pipeline_builder import PipelineBuilder
from mapping_and_aggregation.universe_copy import universe_copy_dag
from mapping_and_aggregation.voldemort_tasks import voldemort_segment_dag, voldemort_cookie_dag, \
    voldemort_email_dag, voldermort_consolidation_dag

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'start_date': datetime(2017, 9, 10),
    'retry_delay': timedelta(minutes=15),
    'email': ['spam@liveintent.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'on_failure_callback': sns_on_failure,
    'on_success_callback': sns_on_success,
    'sla_miss_callback': sns_on_late,
    'sla': timedelta(hours=30),
    'aws_conn_id': 'cph_emr',
    'emr_conn_id': 'cph_emr',
    'ssh_conn_id': 'cph_ssh',
    's3_conn_id': 'cph_s3',
    'job_flow_id': "{{ task_instance.xcom_pull('create_emr_cluster', key='return_value', dag_id='mapping_and_aggregation') }}",
    'override_command': 'cph_command_override',
    'params': {
        'aws_conn_id': 'cph_emr',
        'on_failure_target_arn': 'arn:aws:sns:us-east-1:23423423423423:datapipeline-error',
        'on_failure_sns_subject': 'FAILED: ID-match airflow job',
        'on_failure_sns_msg': 'ID-match airflow job failed.\
                  Please examine and potentially restart.',
        'on_success_target_arn': 'arn:aws:sns:us-east-1:23423423423423:idmatching-success',
        'on_success_sns_subject': 'SUCCESS: ID-match airflow job',
        'on_success_sns_msg': 'ID-match airflow job completed successfully.',
        'on_late_target_arn': 'arn:aws:sns:eu-west-1:23423423423423:mojn-maintenance-critical',
        'on_late_sns_subject': 'SLA_MISSED: ID-match airflow job',
        'on_late_sns_msg': 'ID-match airflow missed its SLA. Please examine.'
    }
}

dag = DAG(
    dag_id='mapping_and_aggregation',
    default_args=default_args,
    schedule_interval='0 1 * * *',
    concurrency=32
)

# Variable.set('cph_emr_configuration', '')
# airflow connections -a --conn_id cph_ssh --conn_uri http://ignore
# airflow connections -a --conn_id cph_ssh_ec2 --conn_uri http://ignore

# STRUCTURE DEFINED BELOW HERE

# create a pipeline builder
p = PipelineBuilder(dag, job_flow_overrides=Variable.get('cph_emr_configuration'), default_args=default_args)

jar_version = Variable.get('cph_jar_version')


# task to push xcoms for the versioned jars for scalding, spark and preprocessor
def push_xcom_jar_version(ds, **context):
    bucket = 's3://releases'
    dwh_scalding_jar = bucket + '/mojn-pipelines/mojn-pipelines_2.11/{}/mojn-pipelines_2.11-{}-assembly.jar'.format(
        jar_version, jar_version)
    spark_jar = bucket + '/spark-dwh/spark-dwh_2.11/{}/spark-dwh_2.11-{}-assembly.jar'.format(
        jar_version, jar_version)
    preprocessor_jar = bucket + '/dwh-preprocessor-data/dwh-preprocessor-data_2.11/{}/dwh-preprocessor-data_2.11-{}-assembly.jar'.format(
        jar_version, jar_version)
    context['task_instance'].xcom_push(key='cph_jars_dwh_scalding', value=dwh_scalding_jar)
    context['task_instance'].xcom_push(key='cph_jars_dwh_spark', value=spark_jar)
    context['task_instance'].xcom_push(key='cph_jars_dwh_preprocessor', value=preprocessor_jar)


broadcast_jar_version = p.python_operator('BroadcastJarVersion', push_xcom_jar_version)

# start hadoop prometheus exporter on the EMR:
p.master_cli('HadoopExporter', 'mapping_and_aggregation/hadoop_exporter.sh')

# list of internal buckets
internal_keys = Variable.get('internal_keys', deserialize_json=True)

cph_test_environment = Variable.get('cph_test_environment')

cost_config = Variable.get('cost_map_var', deserialize_json=True)
# if running on dev environment, build in_out_overrides i.e. command-line
# overrides for overriding internal and external buckets
if cph_test_environment == 'True':
    for k, values in internal_keys.items():
        in_out_overrides = [('--{}Out "{}/"'.format(key, internal_keys[key])) for key in internal_keys] + [
            '--{}In "{}/"'.format(key, internal_keys[key]) for key in internal_keys]

# these could be inlined below, but having them here makes the total s3 dependencies easily seen
yesterday_hash_consolidation_available = p.s3('{}/consolidated-email-hashes/'.format(internal_keys['Mappings']),
                                              'yesterday_ds_nodash', '_SUCCESS')
yesterday_cookie_consolidation_available = p.s3('{}/consolidated-cookies/'.format(internal_keys['Aggregation']),
                                                'yesterday_ds_nodash', '_SUCCESS')
today_cookie_consolidation_available = p.s3('{}/consolidated-cookies/'.format(internal_keys['Aggregation']),
                                            'ds_nodash', '_SUCCESS')
yesterday_email_dmp_available = p.s3('{}/email-hash-dmp/'.format(internal_keys['Dmps']), 'yesterday_ds_nodash',
                                     '_SUCCESS')
yesterday_cookie_info_dmp_available = p.s3('{}/cookie-info-dmp/'.format(internal_keys['Dmps']), 'yesterday_ds_nodash',
                                           '_SUCCESS')
yesterday_living_cookie_available = p.s3('{}/living-cookie/'.format(internal_keys['Mappings']), 'yesterday_ds_nodash',
                                         '_SUCCESS')
yesterday_lidid_fpc_consolidated_cookies_available = p.s3(
    '{}/lidid-fpc-consolidated-cookies/'.format(internal_keys['Aggregation']), 'yesterday_ds_nodash', '_SUCCESS')
yesterday_home_ip_available = p.s3('{}/aggregation/home-ip-visitors/'.format(internal_keys['Mappings']),
                                   'yesterday_ds_nodash', '_SUCCESS')
yesterday_source4_idl_mapping_available = p.s3('{}/source4/partner-mappings/'.format(internal_keys['External']),
                                                'yesterday_ds_nodash', '_SUCCESS')
yesterday_source4Idls_available = p.s3('{}/emailhash-to-idl/'.format(internal_keys['External']), 'yesterday_ds_nodash',
                                        '_SUCCESS')

# these could also be inlined below, but having them here makes their definitions easily seen

bid_processing = p.job('com.mojn.dwh.logs.jobs.BidResponsesDWHJob', '--hdfs', '--date', '{{ ds }}',
                       '--mapping-selectors',
                       's3://mappings/mapping-selectors/current-*.txt').trigger(
    p.master_cli('BidProcessing_SUCCESS',
                 """hadoop dfs -touchz {{ var.json.internal_keys.Logs }}/rtb-auction-bids/{{ ds_nodash }}/_SUCCESS""")
)

hash_consolidation = p.job('com.mojn.dwh.jobs.HashConsolidationDWHJob', '--hdfs', '--date', '{{ ds }}',
                           '--consolidate-request', 'true').trigger(
    p.master_cli('HashConsolidation_DWH_SUCCESS',
                 """hadoop dfs -touchz {{ var.json.internal_keys.Logs }}/li-userver-log/{{ ds_nodash }}/_SUCCESS""")
)

ip_mapping = p.job('com.mojn.dwh.jobs.IPBasedMappingDWHJob', '--hdfs', '--date', '{{ ds }}')
trackings = p.job('com.mojn.dwh.jobs.TrackingsDWHJob', '--hdfs', '--date', '{{ ds }}')
sessions = p.slow_job('com.mojn.dwh.jobs.IdentifierSessionsDWHJob', '--hdfs', '--date', '{{ ds }}')
email_hash_dmp = p.job('com.mojn.dwh.dmp.jobs.EmailHashDMPJob', '--hdfs', '--date', '{{ ds }}')
home_ip = p.big_spark_job('HomeIPDWHSparkJob',
                          ['com.mojn.dwh.jobs.HomeIPDWHSparkJob', '--system', 's3', '--date', '{{ ds }}'],
                          {'spark.default.parallelism': '6288'})
update_source4_IDL = p.job('com.mojn.dwh.external.jobs.Updatesource4IDLJob', '--hdfs', '--date', '{{ ds }}')


gdpr_access = p.spark_job('com.liveintent.dwh.gdpr.GdprAccessDwhJob',
                          job_args=['com.liveintent.dwh.gdpr.GdprAccessDwhJob', '---system', 's3', '--date', '{{ ds }}'],
                          job_uri='s3://releases/com/liveintent/dwh/dwh-sample-experimental_2.11/2.0-upgrade-input-SNAPSHOT/dwh-sample-experimental_2.11-2.0-upgrade-input-SNAPSHOT-assembly.jar'
)

# so sequence should be download sample jar -> ( process accesc request, process deletes - done in cookie info) -> run gdp job
gdpr_job = p.job('com.liveintent.dwh.gdpr.GdprRequestDwhJob', '--hdfs', '--date', '{{ ds }}')

# process the access request after to gdpr main job - as to complete have been computed
gdpr_job.trigger(gdpr_access)

xdata_mapping = p.big_spark_job('ProcessDailyXDataDeltaSparkJob', ['com.liveintent.dwh.partners.source4.jobs.ProcessDailyXDataDeltaSparkJob', '--system', 's3', '--date', '{{ ds }}'], {'spark.default.parallelism': '6288'}).trigger(
                    p.master_cli('XDataMappingGeneration_DWH_SUCCESS',
                                 """hadoop dfs -touchz {{ var.json.internal_keys.External }}/source4/partner-mappings/{{ ds_nodash }}/_SUCCESS""")
                )

cookie_info_dmp = p.big_spark_job('CookieInfoDMPSparkJob', ['com.mojn.dwh.dmp.jobs.CookieInfoDMPSparkJob', '--system', 's3', '--date', '{{ ds }}'], {'spark.default.parallelism': '15000'})

cookie_info_dmp.trigger(gdpr_job)

mapping_aggregation = p.big_spark_job('MappingAggregatorSparkJob',
                                      ['com.mojn.dwh.jobs.MappingAggregatorSparkJob', '--system', 's3', '--date',
                                       '{{ ds }}'], {'spark.default.parallelism': '6288'}).trigger(
    p.big_spark_job('MappingAggregatorSubtypeJob',
                    ['com.mojn.jobs.MappingAggregatorSubtypeJob', '--system', 's3', '--date', '{{ ds }}',
                     '--partitions', '3169'], {'spark.default.parallelism': '6288'}).trigger(
        p.master_cli('DailyMappingAggregation_DWH_SUCCESS',
                     """hadoop dfs -touchz {{ var.json.internal_keys.Aggregation }}/daily-mapping-aggregation/{{ ds_nodash }}/_SUCCESS""")
    )
)

pcg_cross_device = p.slow_job('com.mojn.dwh.pcg.PcgCrossDeviceJob', '--hdfs', '--date', '{{ ds }}')

mapping_discard = p.big_spark_job('MappingDiscardSparkJob',
                                  ['com.mojn.dwh.jobs.MappingDiscardSparkJob', '--system', 's3', '--date', '{{ ds }}'],
                                  {'spark.default.parallelism': '6288'})

# segment tasks start
computed_segments = p.slow_job_latest_is_enough('com.liveintent.dwh.segment.ComputedSegmentDwhJob', '--hdfs', '--date', '{{ ds }}')

ds_cookie_update = p.slow_job('com.liveintent.dwh.segment.DynamicSegmentLididImportDwhJob', '--hdfs', '--date', '{{ ds }}')

bluekai_import = p.slow_job_latest_is_enough('com.liveintent.dwh.segment.BluekaiSegmentImportDwhJob', '--hdfs',
                                             '--date', '{{ ds }}')
source3_import = p.slow_job_latest_is_enough('com.liveintent.dwh.segment.source3SegmentImportDwhJob', '--hdfs',
                                             '--date', '{{ ds }}')
ds_update = p.slow_job('com.liveintent.dwh.segment.DynamicSegmentImportDwhJob', '--hdfs', '--date', '{{ ds }}')

source4_segments = p.slow_job_latest_is_enough('com.liveintent.dwh.segment.source4SegmentImportJob', '--hdfs', '--date', '{{ ds }}',
                                                '--update-processed-dates', 'true')

source5_exelate_import = p.slow_job_latest_is_enough('com.liveintent.dwh.segment.source5ExelateSegmentImportDwhJob',
                                                     '--hdfs', '--date', '{{ ds }}')
# source6 segment tasks
preprocess_source6_files = p.master_cli('Preprocesssource6SegmentFiles',
                                      """java -Daws.profile=source6 -cp /tmp/{{ task_instance.xcom_pull('BroadcastJarVersion', key='cph_jars_dwh_preprocessor', dag_id='mapping_and_aggregation').split('/')[-1] }} com.liveintent.dwh.segment.source6.source6PreProcessor""")

source6_import = p.slow_job_latest_is_enough('com.liveintent.dwh.segment.source6SegmentImportDwhJob', '--hdfs',
                                           '--date', '{{ ds }}')

source6_done = preprocess_source6_files.trigger(source6_import)

# yesterday + live audience tasks
yesterday_live_audience_segments_available = p.s3(
    '{}/segment-membership/liveaudience/email-hash/'.format(internal_keys['Segment']), 'yesterday_ds_nodash',
    '_SUCCESS')

yesterday_live_audience_lidid_segments_available = p.s3(
    '{}/segment-membership/liveaudience/lidid/'.format(internal_keys['Segment']), 'yesterday_ds_nodash', '_SUCCESS')

yesterday_dynamic_segments_available = p.s3(
    '{}/segment-membership/ds/email-hash/'.format(internal_keys['Segment']), 'yesterday_ds_nodash',
    '_SUCCESS')

yesterday_dynamic_lidid_segments_available = p.s3(
    '{}/segment-membership/ds/lidid/'.format(internal_keys['Segment']), 'yesterday_ds_nodash', '_SUCCESS')

la_update = p.slow_job('com.liveintent.dwh.segment.LiveAudienceUpdateDwhJob', '--hdfs', '--date', '{{ ds }}')

la_convert = p.slow_job('com.liveintent.dwh.segment.SegmentMemberShipToLiveAudienceConverterDwhJob', '--hdfs',
                        '--date', '{{ ds }}')

la_cookie_update = p.slow_job('com.liveintent.dwh.segment.LiveAudienceLididImportUpdateDwhJob', '--hdfs', '--date',
                              '{{ ds }}')

# Segment computation + success
derived_segment_computation = p.slow_job_latest_is_enough('com.liveintent.dwh.segment.DeriveSegmentDwhJob',
                                                          '--hdfs', '--date', '{{ ds }}')

derived_segment_success = p.master_cli('DerivedSegment_DWH_SUCCESS',
                                       """hadoop dfs -touchz {{ var.json.internal_keys.Segment }}/derived-segment-membership/{{ ds_nodash }}/_SUCCESS""")

# source1 tasks
source1_aaid_import = p.slow_job_latest_is_enough('com.liveintent.dwh.segment.source1SegmentImportAAIDDwhJob',
                                                 '--hdfs', '--date', '{{ ds }}')

source1_idfa_import = p.slow_job_latest_is_enough('com.liveintent.dwh.segment.source1SegmentImportIDFADwhJob',
                                                 '--hdfs', '--date', '{{ ds }}')

source1_id_import = p.slow_job_latest_is_enough('com.liveintent.dwh.segment.source1SegmentImportsource1IdDwhJob',
                                               '--hdfs', '--date', '{{ ds }}')

# XML task
process_userver_xml = p.master_cli('ProcessUserverXml',
                     """java -cp /tmp/{{ task_instance.xcom_pull('BroadcastJarVersion', key='cph_jars_dwh_preprocessor', dag_id='mapping_and_aggregation').split('/')[-1] }} com.dwh.liveintent.segment.XmlSegmentProcessor --date {{ ds }}""")

# Pcg segment task
pcg_segment = p.slow_job('com.mojn.dwh.pcg.PcgJob', '--hdfs', '--date', '{{ ds }}')

# Make sure one of two flows completed
segments_done = p.custom(DummyOperator, 'segments_done', trigger_rule='one_success')

# DAG construction
(computed_segments + ds_cookie_update  + source2_import + 
 source3_import + ds_update + source5_exelate_import + source6_done + 
 la_cookie_update + source1_aaid_import + source1_idfa_import + 
 source1_id_import).trigger_even_on_failure(derived_segment_computation)

(computed_segments).trigger(pcg_segment)

derived_segment_computation.trigger(derived_segment_success)

(yesterday_live_audience_segments_available + broadcast_jar_version).trigger(la_update)

la_update.trigger(la_convert)

(yesterday_live_audience_lidid_segments_available + broadcast_jar_version).trigger(la_cookie_update)

(yesterday_dynamic_segments_available + broadcast_jar_version).trigger(ds_update)

(yesterday_dynamic_lidid_segments_available + broadcast_jar_version).trigger(ds_cookie_update)

(la_update + derived_segment_success + source4_segments).trigger(segments_done)

# segment tasks end



partner_mappings = p.job_latest_is_enough('com.mojn.dwh.idaas.jobs.BuildPartnerMappingsJob', '--hdfs', '--date',
                                          '{{ ds }}', '--mapping-selectors',
                                          's3://mappings/mapping-selectors/current-*.txt').trigger(
    p.master_cli('BuildPartnerMapping_SUCCESS',
                 """hadoop dfs -touchz {{ var.json.internal_keys.Aggregation }}/partner-mappings/{{ ds_nodash }}/_SUCCESS""")

)

"""
please add this variable in production:

cph_preprocessor_jar: {
  'jar_uri': 's3://releases/dwh-preprocessor-data/dwh-preprocessor-data_2.11/2.0-SNAPSHOT/',
  'jar': 'dwh-preprocessor-data_2.11-2.0-SNAPSHOT-assembly.jar'
}
"""

pre_processor_jar_ready = p.master_cli('DownloadPreprocessorJar',
                                       """aws s3 cp {{ task_instance.xcom_pull('BroadcastJarVersion', key='cph_jars_dwh_preprocessor', dag_id='mapping_and_aggregation') }} /tmp/""")

process_avro_configuration = p.master_cli('PreprocessAvroSchemasForUserver',
                                          """java -cp /tmp/{{ task_instance.xcom_pull('BroadcastJarVersion', key='cph_jars_dwh_preprocessor', dag_id='mapping_and_aggregation').split('/')[-1] }} com.dwh.liveintent.userver.AvroConfigurationGenerator --date {{ ds }}"""
                                          )

# OVERALL STRUCTURE FROM HERE
broadcast_jar_version.trigger(
    update_source4_IDL +
    pre_processor_jar_ready +
    source2_import +
    source3_import +
    source5_exelate_import +
    source1_aaid_import +
    source1_idfa_import +
    source1_id_import +
    source4_segments +
    computed_segments
)
avro_ready = pre_processor_jar_ready.trigger(process_avro_configuration)

pre_processor_jar_ready.trigger(
    preprocess_source6_files +
    process_userver_xml
)

(avro_ready + yesterday_hash_consolidation_available).trigger(bid_processing + hash_consolidation)

bid_processing.trigger(
    p.aws_batch('RtbBidsSummary',
                job_definition='anomalyDetection:2',
                job_params={"entity": "rtb-bids", "date": "{{ yesterday_ds }}"})
)

(yesterday_living_cookie_available + yesterday_lidid_fpc_consolidated_cookies_available).trigger(trackings)

hash_consolidation.trigger(
    ip_mapping + trackings + sessions +
    p.subdag('HashConsolidationVoldemort', voldermort_consolidation_dag) +
    p.subdag('UniverseCopy', universe_copy_dag)
)

(yesterday_email_dmp_available + trackings).trigger(email_hash_dmp)

(ip_mapping + yesterday_home_ip_available).trigger(home_ip)

yesterday_source4Idls_available.trigger(update_source4_IDL)

(
            hash_consolidation + update_source4_IDL + today_cookie_consolidation_available +
            yesterday_source4_idl_mapping_available).trigger(xdata_mapping)

(trackings + bid_processing + yesterday_cookie_info_dmp_available + yesterday_cookie_consolidation_available).trigger(
    cookie_info_dmp
)

sessions.trigger(
    p.aws_batch('ClickSummary',
                job_definition='anomalyDetection:2',
                job_params={"entity": "LI-UServer-click", "date": "{{ yesterday_ds }}"}) +
    p.aws_batch('ConversionSummary',
                job_definition='anomalyDetection:2',
                job_params={"entity": "LI-UServer-conv", "date": "{{ yesterday_ds }}"}) +
    p.aws_batch('ImpressionSummary',
                job_definition='anomalyDetection:2',
                job_params={"entity": "LI-UServer-imp", "date": "{{ yesterday_ds }}"}) +
    p.aws_batch('ImpStaticSummary',
                job_definition='anomalyDetection:2',
                job_params={"entity": "LI-UServer-imp_static", "date": "{{ yesterday_ds }}"}) +
    p.aws_batch('SnowplowSummary',
                job_definition='anomalyDetection:2',
                job_params={"entity": "Snowplow", "date": "{{ yesterday_ds }}"})
)

(ip_mapping + xdata_mapping).trigger(
    p.master_cli('IpMapping_DWH_SUCCESS',
                 """hadoop dfs -touchz {{ var.json.internal_keys.Mappings }}/daily-mappings/{{ ds_nodash }}/_SUCCESS""")
)

mappings = ((ip_mapping + xdata_mapping + cookie_info_dmp).trigger(
    mapping_aggregation
).trigger(
    mapping_discard
) + home_ip).trigger(
    p.master_cli('Aggregation_DWH_SUCCESS',
                 """hadoop dfs -touchz {{ var.json.internal_keys.Aggregation }}/aggregation/mappings/{{ ds_nodash }}/_COMPLETE_SUCCESS""")
)

mappings.trigger(pcg_cross_device)

(mappings + update_source4_IDL).trigger(
    p.subdag_latest_is_enough('CookieVoldemort', voldemort_cookie_dag)
)

seg_vold = p.subdag_latest_is_enough('SegmentVoldemort', voldemort_segment_dag)

(partner_mappings + segments_done).trigger(
    seg_vold
)

mappings.trigger(
    p.spark_job('CrxReachReport',
                main_class='com.twitter.scalding.Tool',
                job_args=['com.mojn.dwh.report.jobs.CrxSummationJob', '--hdfs', '--date', '{{ ds }}'],
                job_spark_conf={'spark.eventLog.enabled': 'false'},
                submit_args={'packages': 'com.databricks:spark-avro_2.11:3.2.0', 'driver-memory': '10G'},
                job_uri='s3://releases/spark-dwh/spark-dwh_2.11/2.0-crx-analysis-SNAPSHOT/spark-dwh_2.11-2.0-crx-analysis-SNAPSHOT-assembly.jar'
                )
)

# build log of source4 onboarding
source4_user_matching_build = p.spark_job(
    'source4UserMatchingReportJobDwh',
    job_args=['com.mojn.dwh.report.jobs.source4UserMatchingReportJobDwh', '--system', 's3', '--date', '{{ ds }}'],
    job_spark_conf={'spark.default.parallelism': '15000'},
    submit_args={'packages': 'com.databricks:spark-avro_2.11:4.0.0'}
)
avro_ready.trigger(source4_user_matching_build)

((cookie_info_dmp + mappings + xdata_mapping).trigger(
    partner_mappings
) + email_hash_dmp).trigger(
    p.subdag_latest_is_enough('EmailHashVoldemort', voldemort_email_dag)
)

def check_state(dag, execution_date, **context):
    # Checks if there are any tasks running before terminating
    session = settings.Session()
    query = "SELECT COUNT(*) FROM task_instance WHERE dag_id = '{}' AND execution_date = '{}' AND state in ('running', 'queued') " \
            "AND operator != 'S3KeySensor' AND task_id != 'Check_running_tasks';".format(dag.dag_id, execution_date)
    count = session.execute(query).fetchall()[0][0]
    if count:
        raise (AirflowException('Cluster about to terminate, but there are still running tasks!'))


avoid_termination = p.after_all_cluster_activity_do(
    p.custom(PythonOperator,
             'Check_running_tasks',
             python_callable=check_state,
             provide_context=True,
             retries=4,
             retry_delay=timedelta(minutes=15)
             )
 )

get_cost_before_termination = avoid_termination.trigger(
    p.custom(HadoopSSHOperator,
              'cost_analysis',
              command='mapping_and_aggregation/cost_analysis_extract.sh')
)

term_cluster = p.custom(EmrTerminateJobFlowOperator,
             'emr_terminate',
             job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}")

get_cost_before_termination.trigger_even_on_failure(term_cluster)

cost_analysis = p.custom(ECSOperator,
                         'cost_report',
                         task_definition=cost_config['ECSConfig']['task_definition'],
                         cluster=cost_config['ECSConfig']['cluster'],
                         overrides={
                             'containerOverrides': [
                                 {
                                     'name': cost_config['ECSConfig']['container_name'],
                                     'environment': [
                                         {
                                             'name': 'DS',
                                             'value': '{{ ds }}'
                                         },
                                         {
                                             'name': 'S3_BUCKET',
                                             'value': cost_config['liveintent_etl_s3_bucket']
                                         }
                                     ]
                                 }
                             ]
                         },
                         aws_conn_id=cost_config['s3liveintent'],
                         region_name=cost_config['ECSConfig']['region_name'],
                         )

term_cluster.trigger(cost_analysis)

etl_redshift = p.custom(PostgresOperator,
                        'redshift_etl',
                        postgres_conn_id='redshift_admin',
                        sql='cost_prod/cost_redshift_etl.sql',
                        params=dict(
                            iam_redshift=Variable.get('redshift_copy_unload_role')
                        ))

cost_analysis.trigger(etl_redshift)

if cph_test_environment == 'True':
  p.materialize_dev(cph_test_environment,
                    in_out_overrides,
                    Variable.get('cph_override_hadoop_operator_tasks'),
                    Variable.get('cph_replace_with_dummy_operators'))
else:
  p.materialize()