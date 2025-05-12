# app/utils/async_s3.py

import os
import logging
from dotenv import load_dotenv
from botocore.config import Config
from botocore.exceptions import ClientError
from aiobotocore.session import get_session
import asyncio

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

logger.info(f"AWS_ACCESS_KEY_ID env → {os.getenv('AWS_ACCESS_KEY_ID')!r}")
logger.info(f"AWS_SECRET_ACCESS_KEY env → {os.getenv('AWS_SECRET_ACCESS_KEY')!r}")
logger.info(f"S3_ENDPOINT_URL env → {os.getenv('S3_ENDPOINT_URL')!r}")
logger.info(f"S3_BUCKET env → {os.getenv('S3_BUCKET')!r}")

class AsyncS3Client:
    def __init__(
        self,
        access_key: str,
        secret_key: str,
        endpoint_url: str,
        bucket_name: str,
        region_name: str | None = None
    ):
        self.bucket_name = bucket_name
        self.client_config = Config(
            signature_version="s3",           # SigV2
            s3={"payload_signing_enabled": False}
        )
        self.session = get_session()
        self.client_kwargs = {
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "endpoint_url": endpoint_url,
            "use_ssl": True,
        }
        if region_name:
            self.client_kwargs["region_name"] = region_name

        logger.info("AsyncS3Client configured (SigV2 + no payload signing).")

    async def put_object(
        self,
        object_key: str,
        body: bytes | str,
        content_type: str | None = None
    ) -> dict:
        """Upload content to S3."""
        try:
            async with self.session.create_client(
                "s3",
                config=self.client_config,
                **self.client_kwargs
            ) as client:
                params: dict = {
                    "Bucket": self.bucket_name,
                    "Key": object_key,
                    "Body": body,
                }
                if content_type:
                    params["ContentType"] = content_type

                resp = await client.put_object(**params)
                status = resp["ResponseMetadata"]["HTTPStatusCode"]
                logger.info(f"Put object '{object_key}', HTTP {status}")
                return resp

        except ClientError as e:
            logger.error(f"Error putting object '{object_key}': {e}")
            raise

    async def get_object(self, object_key: str) -> bytes:
        """Retrieve object content from S3 as bytes."""
        try:
            async with self.session.create_client(
                "s3",
                config=self.client_config,
                **self.client_kwargs
            ) as client:
                resp = await client.get_object(
                    Bucket=self.bucket_name,
                    Key=object_key
                )
                data = await resp["Body"].read()
                logger.info(f"Retrieved '{object_key}', {len(data)} bytes")
                return data

        except ClientError as e:
            logger.error(f"Error retrieving '{object_key}': {e}")
            raise

    async def download_file(self, object_key: str, dest_path: str) -> None:
        """Download S3 object to a local file."""
        data = await self.get_object(object_key)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        with open(dest_path, "wb") as f:
            f.write(data)
        logger.info(f"Downloaded '{object_key}' to '{dest_path}'")

# Initialize a global client from .env
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_ENDPOINT   = os.getenv("S3_ENDPOINT_URL")
S3_BUCKET     = os.getenv("S3_BUCKET")
S3_REGION     = os.getenv("S3_REGION", None)

if not all([S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT, S3_BUCKET]):
    logger.error("Incomplete S3 configuration in .env")
    raise RuntimeError("Missing S3 config variables")

s3_client = AsyncS3Client(
    access_key=S3_ACCESS_KEY,
    secret_key=S3_SECRET_KEY,
    endpoint_url=S3_ENDPOINT,
    bucket_name=S3_BUCKET,
    region_name=S3_REGION
)

