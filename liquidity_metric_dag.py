
from airflow import DAG
from airflow.providers.amazon.aws.operators.ecs import EcsOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'BankA',
    'start_date': datetime(2025, 8, 1),
    'retries': 1,
}

with DAG(
    dag_id='liquidity_ratio_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    extract_liquidity = EcsOperator(
        task_id="extract_liquidity",
        cluster="your-ecs-cluster",
        task_definition="your-task-def",
        launch_type="FARGATE",
        overrides={ 
            "containerOverrides": [{ 
                "name": "your-container-name",
                "command": ["python", "/app/node_extract_liquidity.py"],
                "environment": [{'name': 'BUSINESS_DATE', 'value': '{{ ds }}'},
            {'name': 'ENTITY', 'value': 'BankA'},
            {'name': 'EXECUTION_ID', 'value': '{{ run_id }}'},
            {'name': 'INPUT_DATASETS', 'value': ''},
            {'name': 'OUTPUT_DATASETS', 'value': 'raw_liquidity'}]
            }]
        },
        network_configuration={ 
            "awsvpcConfiguration": {
                "subnets": ['subnet-aaa', 'subnet-bbb'],
                "assignPublicIp": "ENABLED"
            }
        }
    )

    clean_liquidity = EcsOperator(
        task_id="clean_liquidity",
        cluster="your-ecs-cluster",
        task_definition="your-task-def",
        launch_type="FARGATE",
        overrides={ 
            "containerOverrides": [{ 
                "name": "your-container-name",
                "command": ["python", "/app/node_clean_liquidity.py"],
                "environment": [{'name': 'BUSINESS_DATE', 'value': '{{ ds }}'},
            {'name': 'ENTITY', 'value': 'BankA'},
            {'name': 'EXECUTION_ID', 'value': '{{ run_id }}'},
            {'name': 'INPUT_DATASETS', 'value': 'raw_liquidity'},
            {'name': 'OUTPUT_DATASETS', 'value': 'stage1_liquidity'}]
            }]
        },
        network_configuration={ 
            "awsvpcConfiguration": {
                "subnets": ['subnet-aaa', 'subnet-bbb'],
                "assignPublicIp": "ENABLED"
            }
        }
    )

    compute_ratio = EcsOperator(
        task_id="compute_ratio",
        cluster="your-ecs-cluster",
        task_definition="your-task-def",
        launch_type="FARGATE",
        overrides={ 
            "containerOverrides": [{ 
                "name": "your-container-name",
                "command": ["python", "/app/node_compute_ratio.py"],
                "environment": [{'name': 'BUSINESS_DATE', 'value': '{{ ds }}'},
            {'name': 'ENTITY', 'value': 'BankA'},
            {'name': 'EXECUTION_ID', 'value': '{{ run_id }}'},
            {'name': 'INPUT_DATASETS', 'value': 'stage1_liquidity'},
            {'name': 'OUTPUT_DATASETS', 'value': 'final_liquidity_ratio'}]
            }]
        },
        network_configuration={ 
            "awsvpcConfiguration": {
                "subnets": ['subnet-aaa', 'subnet-bbb'],
                "assignPublicIp": "ENABLED"
            }
        }
    )
    extract_liquidity >> clean_liquidity
    clean_liquidity >> compute_ratio
