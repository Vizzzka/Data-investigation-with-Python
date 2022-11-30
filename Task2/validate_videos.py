import pandas as pd
from google.cloud import storage


def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    print(f"Processing file: {file['name']}.")

    path = "gs://{}/{}".format(file['bucket'], file['name'])
    df = pd.read_csv(path)
    df.dropna(inplace=True)
    df = df[df["url"].str.startswith("http")]
    df = df[df["private"].isin([0.0, 1.0])]
    df = df[df["id"] == df["id"].astype(int)]

    text = df.to_parquet()

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(file['bucket'])
    blob = bucket.blob("valid_videos.parquet")

    blob.upload_from_string(str(text))

    print(df)
