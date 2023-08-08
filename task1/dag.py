from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import uuid
import pendulum
import requests
from datetime import date
import json, sys
from airflow.models import Variable


@dag(
    schedule='@daily',
    start_date=pendulum.datetime(2023, 8, 7),
    catchup=False,
    tags=['Webhook'],
)
def webhook_api():
    def generate_data():
        return {
            "user_id": uuid.uuid4(),
            "event_time": date.today()}

    def load_postgres(row):
        """Load data to Postgres database"""
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


    @task()
    def post_webhook(**kwargs):
        gen = kwargs['gen']
        ti = kwargs['ti']
        url = Variable.get("URL")
        response = requests.post(
            url,
            data=json.dumps(gen, default=str),
            headers={"Content-Type": "application/json"})
        if response.status_code == 200:
            res = json.dumps(dict(response.headers))
            print(res)
            ti.xcom_push(key='webhook_json', value=res)
            load_postgres(dict(response.headers))
            return dict(response.headers)
    @task()
    def load_data(**kwargs):
        ti = kwargs['ti']
        json_data = ti.xcom_pull(key='webhook_json', task_ids=['post_webhook'])[0]

    NUM_TASKS = 5
    filename = [generate_data() for _ in range(NUM_TASKS)]
    post_webhook.expand(gen=filename)


webhook_api()
