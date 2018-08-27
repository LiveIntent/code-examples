from collections import Sequence
from functools import reduce
from itertools import (chain,
                       groupby,
                       product)
from operator import itemgetter

from airflow.operators.liveintent_plugin import HadoopAddJobSSHOperator, HadoopSSHOperator
from airflow.operators.liveintent_plugin import SparkOperator
from airflow.operators.liveintent_plugin import EmrCreateJobFlowOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.sensors import S3KeySensor
from airflow.operators.liveintent_plugin import AwsBatchOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators import DummyOperator


class TaskDefinition(object):
    def __init__(self, task_type, task_id, dag, **airflow_args):
        self.task_id = task_id
        self._task_type = task_type
        self._dag = dag
        self._airflow_args = airflow_args
        self._failures_are_not_ok_because = set()
        self._failures_are_ok_because = set()
        self._latest_is_enough_because = set()

    def _get_latest_is_enough(self):
        return bool(self._latest_is_enough_because)

    latest_is_enough = property(_get_latest_is_enough)

    def latest_is_enough_because_of(self, dependency):
        dependency = dependency.task_ids
        for d in dependency:
            self._latest_is_enough_because.add(d)

    def _get_failures_are_ok(self):
        return bool(self._failures_are_ok_because)

    failures_are_ok = property(_get_failures_are_ok)

    def failures_are_ok_because_of(self, dependency):
        dependency = dependency.task_ids
        assert not self._failures_are_not_ok_because, 'Attempted to set "failures are ok" dependency on %s by %s, but it has already been marked as "failures are not ok" by %s' % (
        self.task_id,
        ' and '.join(dependency),
        ' and '.join(self._failures_are_not_ok_because))
        for d in dependency:
            self._failures_are_ok_because.add(d)

    def failures_are_not_ok_because_of(self, dependency):
        dependency = dependency.task_ids
        assert not self._failures_are_ok_because, 'Attempted to set "failures are not ok" dependency on %s by %s, but it has already been marked as "failures are ok" by %s' % (
        self.task_id,
        ' and '.join(dependency),
        ' and '.join(self._failures_are_ok_because))
        for d in dependency:
            self._failures_are_not_ok_because.add(d)

    def _get_airflow_args(self):
        result = {'trigger_rule': 'all_done' if self.failures_are_ok else 'all_success'}
        result.update(self._airflow_args)
        return result

    airflow_args = property(_get_airflow_args)

    def materialize(self):
        try:
            self._task_type(task_id=self.task_id, dag=self._dag, **self.airflow_args)
        except:
            print("Failed to materialize " + str(self))
            raise

    def materialize_dev(self, dwh_test_environment, in_out_overrides, hadoop_override_tasks, tasks_override_dummy):
        try:
            if self.task_id in tasks_override_dummy:
                self.replace_with_dummy_operator()
            elif self.task_id in hadoop_override_tasks:
                if self._task_type.__name__ == 'HadoopSSHOperator':
                    self._airflow_args['command'] += ' ' + ' '.join(in_out_overrides)
                else:
                    self.airflow_args['jar_args'] += in_out_overrides
            self._task_type(task_id=self.task_id, dag=self._dag, **self.airflow_args)
        except:
            print("Failed to materialize " + str(self))
            raise

    def replace_with_dummy_operator(self):
        self = TaskDefinition(DummyOperator, self.task_id, self._dag, **self.airflow_args)
        self._task_type(task_id=self.task_id, dag=self._dag, **self.airflow_args)

    def __str__(self):
        return '%s(%s)%s' % (
        self.task_id, self._task_type.__name__, ' - latest is enough' if self.latest_is_enough else '')


class PipelineBuilder(object):
    # pull versioned jars from xcoms published by job BroacastJarVersion
    JarUri = "{{ task_instance.xcom_pull('BroadcastJarVersion', key='cph_jars_dwh_scalding', dag_id='mapping_and_aggregation') }}"

    dwh_spark_jar = "{{ task_instance.xcom_pull('BroadcastJarVersion', key='cph_jars_dwh_spark', dag_id='mapping_and_aggregation') }}"

    # spark.default.parallelism and spark.executor.instances depends on the cluster size
    # the current value are for 50 machine cluster
    spark_conf = {'spark.dynamicAllocation.enabled': 'true',
                  'spark.dynamicAllocation.executorIdleTimeout': '2m',
                  'spark.dynamicAllocation.minExecutors': '1',
                  'spark.io.compression.lz4.blockSize': '512KB',
                  'spark.shuffle.service.index.cache.entries': '2048',
                  'spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version': '2',
                  'spark.driver.maxResultSize': '0',
                  'spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped': 'true',
                  'spark.rdd.compress': 'true',
                  'spark.network.timeout': '1000',
                  'spark.shuffle.compress': 'true',
                  'spark.hadoop.mapred.output.fileoutputformat.compress': 'true',
                  'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
                  'spark.dynamicAllocation.maxExecutors': '300000',
                  'spark.hadoop.mapred.output.fileoutputformat.compression.codec': 'org.apache.hadoop.io.compress.GzipCodec',
                  'spark.yarn.executor.memoryOverhead': '3072',
                  'spark.hadoop.mapred.output.compress': 'true',
                  'spark.executor.memory': '20g',
                  'spark.driver.memory': '40g',
                  'spark.driver.cores': '6',
                  'spark.yarn.driver.memoryOverhead': '15360',
                  'spark.executor.cores': '3',
                  'spark.shuffle.registration.maxAttempts': '5',
                  'spark.speculation': 'false',
                  'spark.hadoop.validateOutputSpecs': 'false',
                  'spark.hadoop.mapred.output.compression.codec': 'org.apache.hadoop.io.compress.GzipCodec',
                  'spark.shuffle.io.backLog': '8192',
                  'spark.shuffle.registration.timeout': '2m',
                  'spark.shuffle.service.enabled': 'true',
                  'spark.shuffle.file.buffer': '1m',
                  'spark.file.transferTo': 'false',
                  'spark.eventLog.enabled': 'true',
                  'spark.eventLog.dir': 's3://debug-liveintent-com/spark-history-server',
                  'spark.unsafe.sorter.spill.reader.buffer.size': '1m',
                  'spark.shuffle.unsafe.file.output.buffer': '5m',
                  'spark.executor.extraJavaOptions': '"-XX:ParallelGCThreads=4 -XX:+UseParallelGC"'
                  }

    def __init__(self, dag, job_flow_overrides, default_args, main_dag=True):
        self._dag = dag
        self._tasks = {}
        self._cluster_tasks = set()
        self._dependencies = set()
        self.default_args = default_args
        self.main_dag = main_dag

        class TaskSet(Sequence):
            def __init__(slf, *task_ids):
                slf._task_ids = task_ids

            def trigger(slf, others):
                self.trigger(slf, others)
                return slf + others

            def trigger_even_on_failure(slf, others):
                self.trigger_even_on_failure(slf, others)
                return slf + others

            def __add__(slf, other):
                return TaskSet(*slf._task_ids + other._task_ids)

            def __getitem__(slf, k):
                return TaskSet(slf._task_ids[k])

            def __len__(slf):
                return len(slf._task_ids)

            def _get_task_id(slf):
                assert len(slf) == 1
                return slf._task_ids[0]

            task_id = property(_get_task_id)

            def _get_task_ids(slf):
                return list(slf._task_ids)

            task_ids = property(_get_task_ids)

            def __eq__(slf, other):
                return sorted(slf._task_ids) == sorted(other._task_ids)

            def __hash__(slf):
                return hash(sorted(slf._task_ids))

            def _get_latest_is_enough(slf):
                return any(self[t].latest_is_enough for t in slf._task_ids)

            latest_is_enough = property(_get_latest_is_enough)

            def set_latest_is_enough(slf, triggering_set):
                for t in slf._task_ids:
                    self[t].latest_is_enough_because_of(triggering_set)

        self.TaskSet = TaskSet

        if self.main_dag:
            self._cluster_ready = self.custom(EmrCreateJobFlowOperator,
                                              'create_emr_cluster',
                                              job_flow_overrides=job_flow_overrides)

    def _get_dependency_pairs(self):
        if self.main_dag:
            edges = set(chain(self._dependencies, (('create_emr_cluster', t) for t in self._cluster_tasks)))
        else:
            edges = set(chain(self._dependencies))
        graph = {start: set(e[1] for e in es) for start, es in groupby(sorted(edges), key=itemgetter(0))}

        def reachable(a):
            result = set()
            queue = list(graph.get(a, ()))
            while queue:
                b = queue.pop()
                result.add(b)
                queue += [c for c in graph.get(b, ()) if c not in result]
            return result

        for node, direct_descendants in graph.items():
            for d in direct_descendants:
                for t in reachable(d) & direct_descendants:
                    edges.discard((node, t))
        return edges

    dependency_pairs = property(_get_dependency_pairs)

    def __getitem__(self, task_id):
        task_id = getattr(task_id, 'task_id', task_id)
        return self._tasks[task_id]

    def trigger(self, a, b):
        if a.latest_is_enough:
            b.set_latest_is_enough(a)
        for t1, t2 in product(a, b):
            self._dependencies.add((t1.task_id, t2.task_id))
        for t in b:
            self[t].failures_are_not_ok_because_of(a)

    def trigger_even_on_failure(self, a, b):
        if a.latest_is_enough:
            b.set_latest_is_enough(a)
        for t1, t2 in product(a, b):
            self._dependencies.add((t1.task_id, t2.task_id))
        for t in b:
            self[t].failures_are_ok_because_of(a)

    def materialize(self):
        for t in self._tasks.values():
            t.materialize()
        for t1, t2 in self.dependency_pairs:
            self._dag.set_dependency(self._clean(t1), self._clean(t2))

    def materialize_dev(self, dwh_test_environment, in_out_overrides, hadoop_override_tasks, tasks_override_dummy):
        for t in self._tasks.values():
            t.materialize_dev(dwh_test_environment, in_out_overrides, hadoop_override_tasks, tasks_override_dummy)
        for t1, t2 in self.dependency_pairs:
            self._dag.set_dependency(self._clean(t1), self._clean(t2))

    def _clean(self, key):
        import string
        return ''.join(filter(lambda c: c in string.ascii_letters + "0123456789_-", key))

    def __str__(self):
        result = '\n'.join(str(t) for t in self._tasks.values())
        result += '\n\n' + '\n'.join('%s ---> %s' % d for d in sorted(self.dependency_pairs))
        return result

    def _add(self, task_id, task_type, **airflow_args):
        if task_id not in self._tasks:
            self._tasks[task_id] = TaskDefinition(task_type, self._clean(task_id), self._dag, **airflow_args)
        return self.TaskSet(task_id)

    def _cluster_add(self, task_id, task_type, **airflow_args):
        result = self._add(task_id, task_type, **airflow_args)
        self._cluster_tasks.add(result.task_id)
        return result

    def slow_job(self, jobName, *extra_args, **airflow_args):

        return self._cluster_add(jobName.split('.')[-1],
                                 HadoopAddJobSSHOperator,
                                 jar_uri=self.JarUri,
                                 jar_args=['-Dmapreduce.job.queuename=root.best_effort_jobs', jobName] + list(
                                     extra_args),
                                 **airflow_args)

    def ssh_job(self, task_name, **airflow_args):
        return self._cluster_add(task_name,
                                 SSHOperator,
                                 **airflow_args)

    def job(self, jobName, *extra_args, **airflow_args):
        return self._cluster_add(jobName.split('.')[-1],
                                 HadoopAddJobSSHOperator,
                                 jar_uri=self.JarUri,
                                 jar_args=[jobName] + list(extra_args),
                                 **airflow_args)

    def master_cli(self, name, command, **airflow_args):
        return self._cluster_add(name,
                                 HadoopSSHOperator,
                                 command=command,
                                 **airflow_args)

    def big_spark_job(self, job_name, job_args, job_spark_conf=None, job_uri=None, **airflow_args):
        conf = dict(self.spark_conf)

        if job_spark_conf:
            conf.update(job_spark_conf)

        return self.spark_job(job_name, job_args, conf, {'deploy-mode': 'cluster', 'master': 'yarn'}, job_uri,
                              **airflow_args)

    def spark_job(self, job_name, job_args, job_spark_conf=None, submit_args=None, job_uri=None,
                  main_class='com.liveintent.dwh.spark.util.SparkJobRunner', **airflow_args):

        if not job_uri:
            job_uri = self.dwh_spark_jar

        return self._cluster_add(job_name,
                                 SparkOperator,
                                 submit_args=submit_args,
                                 conf=job_spark_conf,
                                 main_class=main_class,
                                 jar_uri=job_uri,
                                 jar_args=job_args or self.dwh_spark_jar,
                                 **airflow_args)

    def subdag(self, name, subdag, **airflow_args):
        return self._cluster_add(name,
                                 SubDagOperator,
                                 subdag=subdag(self._dag, name, self.default_args),
                                 **airflow_args)

    def s3(self, key, time_variable, success_file, **airflow_args):
        key = key + '{{ ' + time_variable + ' }}/' + success_file
        path, time_variable = key.split('{{')[0:2]
        time_variable = time_variable.split('}}')[0].strip()
        path = path.split('//')[1].replace('liveintent-com', '').replace('liveintent-dev/', '').rstrip(
            '/').replace('/', '-').replace(" ", "_")
        task_id = {'yesterday_ds_nodash': 'Yesterday', 'ds_nodash': 'Today'}[
                      time_variable] + ' ' + path.lower() + ' available'
        return self._add(task_id,
                         S3KeySensor,
                         bucket_key=key,
                         **airflow_args)

    def ext_sensor(self, task_id, ext_dag_name, ext_task_id):
        return self._add(task_id,
                         ExternalTaskSensor,
                         external_dag_id='{}.{}'.format(self._dag.dag_id, ext_dag_name),
                         external_task_id=ext_task_id,
                         timeout=60
                         )

    def python_operator(self, task_id, python_callable, **airflow_args):
        return self._cluster_add(task_id,
                                 PythonOperator,
                                 python_callable=python_callable,
                                 provide_context=True,
                                 **airflow_args)

    def aws_batch(self, name, **airflow_args):
        return self._add(name,
                         AwsBatchOperator,
                         job_name=name,
                         job_queue='li-batch',
                         **airflow_args)

    def custom(self, task_type, name, **airflow_args):
        return self._add(name,
                         task_type,
                         **airflow_args)

    def after_all_cluster_activity_do(self, *tasks):
        return self.TaskSet(*list(self._cluster_tasks)).trigger_even_on_failure(reduce(lambda a, b: a + b, tasks))

    def getdag(self):
        return self._dag

    def __getattr__(self, attribute_name):
        if attribute_name.endswith('_latest_is_enough'):
            func = getattr(self, attribute_name[:-17])

            def just_latest_wrapper(*args, **kwargs):
                result = func(*args, **kwargs)
                self[result].latest_is_enough_because_of(result)
                return result

            return just_latest_wrapper

        raise AttributeError('No such attribute ' + attribute_name)