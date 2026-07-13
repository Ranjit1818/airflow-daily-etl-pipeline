from airflow.sdk import DAG 
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from datetime import datetime

def decide_flow(**context):
    if context['templates_dict']['value'] < 100:
        return "checkout_task"
    else:
        return "merge_flow_task"

dag = DAG(dag_id="pipeline_layout_example", start_date=datetime(2026,7,1), schedule="*/2 * * * *")

start = EmptyOperator(dag=dag,task_id="start_task")


user_login_page = BashOperator(dag=dag, task_id="user_login_page_task", bash_command="echo 'user_login_page - fething data'")

product_page = BashOperator(dag=dag, task_id="product_page_task", bash_command="echo 'Product page - fething data'")

checkout_page = BashOperator(dag=dag, task_id="checkout_page_task", bash_command="echo 'checkout_page - fething data'")

read_raw = BranchPythonOperator(dag=dag,task_id="decide_flow", python_callable=decide_flow, templates_dict={"value": 100})

checkout_flow = BashOperator(dag=dag, task_id="checkout_task", bash_command = "echo 'Alert - the amount is less than threshold'")

merge_flow = BashOperator(dag= dag, task_id="merge_flow_task", bash_command="echo 'Merging the all webistedata...'")

alarming_situtation = BashOperator(dag=dag, task_id="alarm_task", bash_command="echo 'Warning -- the amount is less than 100--'")

notify = BashOperator(dag=dag, task_id="notify_task", bash_command="echo 'Today data is ready to use ---'")

end = EmptyOperator(dag=dag,task_id="end_task", trigger_rule="none_failed")

start >> [user_login_page, product_page, checkout_page] >> read_raw >> [checkout_flow, merge_flow]
merge_flow >> notify >> end
checkout_flow >> alarming_situtation >> end







 