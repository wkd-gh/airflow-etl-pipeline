from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from google.cloud import secretmanager

GCP_PROJECT_ID = "equity-derivative-etl"


def get_secret(secret_id: str) -> str:
    hook = GoogleBaseHook(gcp_conn_id="google_cloud_conn")
    credentials, _ = hook.get_credentials_and_project_id()
    client = secretmanager.SecretManagerServiceClient(credentials=credentials)
    name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_id}/versions/latest"
    return client.access_secret_version(request={"name": name}).payload.data.decode("UTF-8")
