from airflow.models import DagRun


def get_most_recent_dag_run(dag_id):
    """
    Get the most recent dag run from a DAG id.

    Param:
        dag_id: String with id of the dag.

    Return a airflow.models.DagRun object or None.

    Link to airflow.models.DagRun class: https://github.com/apache/airflow/blob/main/airflow/models/dagrun.py
    """
    dag_runs = DagRun.find(dag_id=dag_id)
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    return dag_runs[0] if dag_runs else None
