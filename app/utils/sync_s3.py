# app/utils/s3_sync.py

import os
import logging
from dotenv import load_dotenv
from botocore.session import Session
from botocore.config import Config
from botocore.exceptions import ClientError

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class S3ClientSync:
    def __init__(
        self,
        access_key: str,
        secret_key: str,
        endpoint_url: str,
        bucket_name: str,
        region_name: str | None = None
    ):
        self.bucket_name = bucket_name
        config = Config(
            signature_version="s3",
            s3={"payload_signing_enabled": False},
        )

        session = Session()
        session.set_credentials(access_key, secret_key, token=None)
        self.client = session.create_client(
            service_name="s3",
            region_name=region_name,
            endpoint_url=endpoint_url,
            config=config,
        )
        logger.info("S3ClientSync initialized for bucket %r", bucket_name)

    def put_object(self, object_key: str, body: bytes | str, content_type: str | None = None) -> dict:
        params = {"Bucket": self.bucket_name, "Key": object_key, "Body": body}
        if content_type:
            params["ContentType"] = content_type
        try:
            resp = self.client.put_object(**params)
            logger.info("Uploaded %r (HTTP %s)", object_key, resp["ResponseMetadata"]["HTTPStatusCode"])
            return resp
        except ClientError as e:
            logger.error("Error uploading %r: %s", object_key, e)
            raise

    def get_object(self, object_key: str) -> bytes:
        try:
            resp = self.client.get_object(Bucket=self.bucket_name, Key=object_key)
            data = resp["Body"].read()
            logger.info("Fetched %r (%d bytes)", object_key, len(data))
            return data
        except ClientError as e:
            logger.error("Error fetching %r: %s", object_key, e)
            raise

    def download_file(self, object_key: str, dest_path: str) -> None:
        """Download via get_object → write bytes to file."""
        data = self.get_object(object_key)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        with open(dest_path, "wb") as f:
            f.write(data)
        logger.info("Downloaded %r to %r", object_key, dest_path)

# initialize singleton
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_ENDPOINT   = os.getenv("S3_ENDPOINT_URL")
S3_BUCKET     = os.getenv("S3_BUCKET")
S3_REGION     = os.getenv("S3_REGION")

if not all([S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT, S3_BUCKET]):
    raise RuntimeError("Missing S3 config variables")

S3_CLIENT_SYNC = S3ClientSync(
    access_key=S3_ACCESS_KEY,
    secret_key=S3_SECRET_KEY,
    endpoint_url=S3_ENDPOINT,
    bucket_name=S3_BUCKET,
    region_name=S3_REGION,
)