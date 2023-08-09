from airflow.decorators import dag, task
from pendulum import datetime
import requests
import json
import uuid
from datetime import date
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import ndjson


@dag(start_date=datetime(2023, 8, 8), schedule=None, catchup=False)
def webhook_api_fuck():
    # upstream task returning a list or dictionary
    def generate_data():
        """Generate Json file"""
        gen = {
            "user_id": uuid.uuid4(),
            "event_time": date.today()}
        return json.dumps(gen, default=str)

    @task
    def generate_tasks(numb: int = 1):
        """Create number of tasks"""
        return [generate_data() for _ in range(numb)]

    # dynamically mapped task iterating over list returned by an upstream task
    @task
    def post_webhook(**kwargs):
        """Post request which sends data to https://webhook.site/ """
        ti = kwargs['ti']
        generated = kwargs['generated']
        url = Variable.get("webhook_url")
        response = requests.post(
            url, data=generated, headers={"Content-Type": "application/json"})
        if response.status_code == 200:
            ti.xcom_push(key='value from pusher 1', value=dict(response.headers))
            return dict(response.headers)

    @task
    def to_json(**kwargs):
        """Load data to ndjson"""
        ti = kwargs['ti']
        row = ti.xcom_pull(
            task_ids="post_webhook",
            key="value from pusher 1")
        with open("/Users/ihor/Apps/Airflow/sample.json", "w") as outfile:
            outfile.write(ndjson.dumps(row))

    @task
    def load_postgres(**kwargs):
        """Load data to Postgres database"""
        ti = kwargs['ti']
        row = ti.xcom_pull(
            task_ids="post_webhook",
            key="value from pusher 1")
        try:
            hook = PostgresHook(postgres_conn_id="postgres_webhook")
            conn = hook.get_conn()
            with conn.cursor() as cursor:
                cursor.execute(f"""INSERT INTO headers_webhook (
                        Server, request_id, token_id, date, Content_Type, 
                        Transfer_Encoding, Vary)
                            VALUES(
                            '{row['Server']}',
                            '{row['X-Request-Id']}',
                            '{row['X-Token-Id']}',
                            '{row['Date']}',
                            '{row['Content-Type']}',
                            '{row['Transfer-Encoding']}',
                            '{row['Vary']}'
                            )ON CONFLICT (request_id) DO NOTHING;""")
                conn.commit()
        except Exception as exception:
            print(f"Failed query execution. Exception: {exception}")

    filename = generate_tasks()
    post_webhook.expand(generated=filename) >> load_postgres()


webhook_api_fuck()
