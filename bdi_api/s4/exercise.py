import gzip
import json
import os
from typing import Annotated

import boto3
import httpx
from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)


@s4.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
    Limits the number of files to download.
    You must always start from the first the page returns and
    go in ascending order in order to correctly obtain the results.
    I'll test with increasing number of files starting from 100.""",
        ),
    ] = 100,
) -> str:
    """Same as s1 but store to an aws s3 bucket taken from settings
    and inside the path raw/day=20231101/
    """
    base_url = settings.source_url + "/2023/11/01/"
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"

    links = []
    for hour in range(24):
        for minute in range(60):
            for second in range(0, 60, 5):
                links.append(f"{hour:02d}{minute:02d}{second:02d}Z.json.gz")

    links = links[:file_limit]

    s3_client = boto3.client("s3")

    for link in links:
        file_url = base_url + link
        file_response = httpx.get(file_url, follow_redirects=True)
        s3_key = s3_prefix_path + link
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=file_response.content,
        )

    return "OK"


@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Obtain the data from AWS s3 and store it in the local prepared directory"""
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"
    prepared_dir = settings.prepared_dir

    os.makedirs(prepared_dir, exist_ok=True)

    s3_client = boto3.client("s3")

    response = s3_client.list_objects_v2(
        Bucket=s3_bucket,
        Prefix=s3_prefix_path,
    )

    if "Contents" not in response:
        return "OK"

    for obj in response["Contents"]:
        s3_key = obj["Key"]
        filename = os.path.basename(s3_key)

        s3_response = s3_client.get_object(
            Bucket=s3_bucket,
            Key=s3_key,
        )
        compressed_data = s3_response["Body"].read()

        try:
            json_data = gzip.decompress(compressed_data)
        except gzip.BadGzipFile:
              json_data = compressed_data
        data = json.loads(json_data)

        aircraft_list = data.get("aircraft", [])

        output_filename = filename.replace(".json.gz", ".json")
        output_path = os.path.join(prepared_dir, output_filename)

        with open(output_path, "w") as f:
            json.dump(aircraft_list, f)

    return "OK"