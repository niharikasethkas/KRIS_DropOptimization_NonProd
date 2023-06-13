import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

DEFAULT_ARGS = {
    "owner": "DDRPF",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 12, 9, 30),
    "email": ['siddharth.dixit@kas-services.com'],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "provide_context": True
}

envs = {"AWS_DEFAULT_REGION": "ap-southeast-2", "ENVIRONMENT_MIN": os.environ["ENVIRONMENT_MIN"]}
DAG_ID = DAG('KRIS_Dropoptimization_NonProd', schedule_interval='15 0 * * *', catchup=False, default_args=DEFAULT_ARGS) # running dags at 14:00 everyday
compute_resources = {'request_cpu': '1', 'request_memory': '8Gi', 'limit_cpu': '1', 'limit_memory': '16Gi'}

affinity = {
  'nodeAffinity': {
    'requiredDuringSchedulingIgnoredDuringExecution': {
      'nodeSelectorTerms': [{
        'matchExpressions': [{
          "key": "nodefleet",
          "operator": "In",
          'values': ["airflow"]
          },
          {
            "key": "node.kubernetes.io/instance-type",
            "operator": "In",
            "values": ["t3a.xlarge"]
        }]
      }]
    }
  }
}

tolerations = [
  {
    'key': 'airflow',
    'operator': 'Exists',
    'effect': 'NoSchedule'
  }
]



T1 = KubernetesPodOperator(namespace='airflow',
                           image="847029211010.dkr.ecr.ap-southeast-2.amazonaws.com/analytics/kris_do_nonprod:airflow_prod",
                           image_pull_policy='Always',
                           cmds=["python", "../model/KRIS_Data_Preparation.py"],
                           arguments=[""],
                           name="data_preparation",
                           task_id="data_preparation",
                           get_logs=True,
                           is_delete_operator_pod=True,
                           env_vars=envs,
                           affinity=affinity,
                           tolerations=tolerations,
                           resources=compute_resources,
                           startup_timeout_seconds=1000,
                           dag=DAG_ID
                           )

T2 = KubernetesPodOperator(namespace='airflow',
                           image="847029211010.dkr.ecr.ap-southeast-2.amazonaws.com/analytics/kris_do_nonprod:airflow_prod",
                           image_pull_policy='Always',
                           cmds=["python", "../model/KRIS_User_input.py"],
                           arguments=[""],
                           name="user_input",
                           task_id="user_input",
                           get_logs=True,
                           is_delete_operator_pod=True,
                           env_vars=envs,
                           affinity=affinity,
                           tolerations=tolerations,
                           resources=compute_resources,
                           startup_timeout_seconds=1000,
                           dag=DAG_ID
                           )


T3 = KubernetesPodOperator(namespace='airflow',
                           image="847029211010.dkr.ecr.ap-southeast-2.amazonaws.com/analytics/kris_do_nonprod:airflow_prod",
                           image_pull_policy='Always',
                           cmds=["python", "../model/KRIS_drop_frequency_optimization_functions_ortools.py"],
                           arguments=[""],
                           name="optimization_func",
                           task_id="optimization_func",
                           get_logs=True,
                           is_delete_operator_pod=True,
                           env_vars=envs,
                           affinity=affinity,
                           tolerations=tolerations,
                           resources=compute_resources,
                           startup_timeout_seconds=1000,
                           dag=DAG_ID
                           )


T4 = KubernetesPodOperator(namespace='airflow',
                           image="847029211010.dkr.ecr.ap-southeast-2.amazonaws.com/analytics/kris_do_nonprod:airflow_prod",
                           image_pull_policy='Always',
                           cmds=["python", "../model/KRIS_run_optimization_program_ortools.py"],
                           arguments=[""],
                           name="run_optimization_func",
                           task_id="run_optimization_func",
                           get_logs=True,
                           is_delete_operator_pod=True,
                           env_vars=envs,
                           affinity=affinity,
                           tolerations=tolerations,
                           resources=compute_resources,
                           startup_timeout_seconds=1000,
                           dag=DAG_ID
                           )




T1 >> T2 >> T3 >> T4 


# T0 >> [T1, T6] 
# T1 >> [T2, T4] >> T5
# T6 >> T7
