from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import cross_downstream
from ETL import main
from ETL.main import (create_tables, 
                      cr_ns, cr_hd, cr_kok, cr_hns,
                        pr_homeshop, pr_kok,
                        emb_kok, emb_homeshop,
                        pred_all,
)
with DAG(
    dag_id="uhok_pipeline",
    start_date=datetime(2025, 9, 10, 20, 0),
    schedule="0 2,5,8,11,14,17,20,23 * * *",
    max_active_runs=4,
    catchup=False,
) as dag :
    task1 = PythonOperator(
            task_id='create_tables',
            python_callable = create_tables,
            dag=dag
        )
    task2 = PythonOperator(
            task_id='cr_ns',
            python_callable = cr_ns,
            dag=dag
        )
    task3 = PythonOperator(
            task_id='cr_hd',
            python_callable = cr_hd,
            dag=dag
        )
    task4 = PythonOperator(
            task_id='cr_kok',
            python_callable = cr_kok,
            dag=dag
        )
    task5 = PythonOperator(
            task_id='cr_hns',
            python_callable = cr_hns,
            dag=dag
        )
    task6 = PythonOperator(
            task_id='pr_homeshop',
            python_callable = pr_homeshop,
            dag=dag
        )
    task7 = PythonOperator(
            task_id='pr_kok',
            python_callable = pr_kok,
            dag=dag
        )
    task8 = PythonOperator(
            task_id='emb_kok',
            python_callable = emb_kok,
            dag=dag
        )
    task9 = PythonOperator(
            task_id='emb_homeshop',
            python_callable = emb_homeshop,
            dag=dag
        )
    task10 = PythonOperator(
            task_id='pred_all',
            python_callable = pred_all,
            dag=dag
        )
    create_table_task = task1
    crawl_all_task = [task2, task3, task4, task5]
    prpc_all_task = [task6, task7]
    emb_all_task = [task8, task9]
    pred_all_task = task10

    # 1) create_tables 끝나면 크롤 4개가 동시에 시작
    create_table_task >> crawl_all_task

    # 2) 크롤 4개가 모두 끝나면 전처리 2개를 동시에 시작 (4 x 2 모두-대-모두)
    cross_downstream(crawl_all_task, prpc_all_task)

    # 3) 전처리 2개가 모두 끝나면 임베딩 2개를 동시에 시작 (2 x 2 모두-대-모두)
    cross_downstream(prpc_all_task, emb_all_task)

    # 4) 임베딩 2개가 모두 끝나면 pred_all 실행
    emb_all_task >> pred_all_task

if __name__ == "__main__":
    dag.cli()