import json
import time
from zipfile import ZipFile
import uuid
import os
from decimal import Decimal

import boto3

class DataLakeClient:
    # This class is for performing operations about Data Lake, i.e. related to S3.
    # Used by multiple Lambda functions:
    # - Comsumer
    # - SQS Lambda
    # - Backend API: handle_alerts(), /export/*

    def __init__(self, bucket: str) -> None:
        # param bucket: Name of the S3 bucket that this client builds on.

        self.bucket = bucket

    def build_record_key(self, detail: dict) -> str:
        # Build the formate of the name and path in bucket for data lake.
        # Example: raw/2025/11/09/device_id=dev-001/period_no=000001.json

        t = int(time.time())
        ts = time.gmtime(t)
        y, m, d = ts.tm_year, ts.tm_mon, ts.tm_mday
        key = f'raw/{y:04d}/{m:02d}/{d:02d}/device_id={detail["device_id"]}/period_no={int(detail["period_no"]):06d}.json'
        return key

    def save_record(self, detail: dict, key: str | None = None) -> str:
        # Save the data into S3 bucket in correct formate

        if key is None:
            key = self.build_record_key(detail)

        body = json.dumps(detail, separators=(",", ":"), ensure_ascii=False)

        s3 = boto3.client("s3")
        s3.put_object(Bucket=self.bucket, Key=key, Body=body.encode("utf-8"))

        return key

    def list_latest_details(self, prefix: str = "raw/", limit: int = 200):
        # Used in Backend API Lambda: /alerts
        # To return param limit number of objects in the param prefix

        s3 = boto3.client("s3")
        resp = s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        objs = resp.get("Contents", [])

        objs_sorted = sorted(objs, key=lambda x: x["LastModified"], reverse=True)[:limit]

        items = []
        for obj in objs_sorted:
            key = obj["Key"]
            body = s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
            try:
                detail = json.loads(body.decode("utf-8"))
            except Exception:
                continue
            detail["s3_key"] = key
            items.append(detail)

        return items

    def export_bucket_to_zip(self, prefix: str, export_prefix: str, export_bucket: str):
        # read from self.bucket+prefix, make a zip and upload to export_bucket
        # return URL (using export_prefix in export key)

        s3 = boto3.client("s3")
        resp = s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        objs = resp.get("Contents", [])

        if not objs:
            return None

        tmp_zip_path = f'/tmp/export-{uuid.uuid4().hex}.zip'

        with ZipFile(tmp_zip_path, "w") as zf:
            for obj in objs:
                key = obj["Key"]
                data = s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
                zf.writestr(key, data)

        export_key = f'{export_prefix}/export-{int(time.time())}.zip'
        with open(tmp_zip_path, "rb") as f:
            s3.put_object(Bucket=export_bucket, Key=export_key, Body=f)

        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": export_bucket, "Key": export_key},
            ExpiresIn=3600, # valid for 1 hour
        )
        return url

