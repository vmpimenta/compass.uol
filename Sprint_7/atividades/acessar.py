import boto3
from datetime import date

data = date.today()

client = boto3.client('s3',
                      aws_access_key_id="****************",
                      aws_secret_access_key="*************"
                      )

client.upload_file("movies.csv", "pimprawzone", (f"Raw/Local/CSV/Movies/{data.year}/{data.month}/{data.day}/movies.csv"))

client.upload_file("series.csv", "pimprawzone", (f"Raw/Local/CSV/Series/{data.year}/{data.month}/{data.day}/series.csv"))
