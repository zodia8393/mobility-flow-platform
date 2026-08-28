from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from mobility_flow.config import Settings, get_settings


@dataclass(frozen=True, slots=True)
class ObjectInfo:
    key: str
    etag: str
    size: int


class ObjectStore:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        kwargs: dict[str, object] = {
            "service_name": "s3",
            "region_name": self.settings.aws_region,
        }
        if self.settings.s3_endpoint_url:
            kwargs.update(
                endpoint_url=self.settings.s3_endpoint_url,
                aws_access_key_id=self.settings.aws_access_key_id,
                aws_secret_access_key=self.settings.aws_secret_access_key,
                config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
            )
        self.client = boto3.client(**kwargs)
        self.bucket = self.settings.s3_bucket

    def ensure_bucket(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.bucket)
            return
        except ClientError as exc:
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status not in {403, 404}:
                raise
            if status == 403:
                raise
        params: dict[str, object] = {"Bucket": self.bucket}
        if self.settings.is_aws_object_store and self.settings.aws_region != "us-east-1":
            params["CreateBucketConfiguration"] = {
                "LocationConstraint": self.settings.aws_region
            }
        self.client.create_bucket(**params)

    def upload_bytes(self, key: str, body: bytes | BinaryIO, content_type: str) -> None:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType=content_type)

    def upload_file(self, source: Path, key: str) -> None:
        self.client.upload_file(str(source), self.bucket, key)

    def download_file(self, key: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(self.bucket, key, str(destination))

    def list_objects(self, prefix: str) -> Iterator[ObjectInfo]:
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                yield ObjectInfo(
                    key=item["Key"],
                    etag=item.get("ETag", "").strip('"'),
                    size=item.get("Size", 0),
                )

    def count_objects(self, prefix: str) -> int:
        return sum(1 for _ in self.list_objects(prefix))
