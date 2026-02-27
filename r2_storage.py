"""Cloudflare R2 操作用のクラス（boto3 S3 互換）。"""

from typing import Optional

import boto3
from botocore.config import Config

from config import (
    R2_ACCESS_KEY_ID,
    R2_BUCKET,
    R2_ENDPOINT,
    R2_SECRET_ACCESS_KEY,
)


def _default_config() -> Config:
    """R2 互換の boto3 設定（boto3 1.36 との互換のため）。"""
    return Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
        region_name="auto",
    )


class R2Storage:
    """Cloudflare R2 のアップロード・ダウンロード・一覧・削除を行うクラス。"""

    def __init__(self, bucket_name: Optional[str] = None):
        self.bucket_name = bucket_name or R2_BUCKET
        self.client = boto3.client(
            service_name="s3",
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            config=_default_config(),
        )

    def list_files(self, prefix: str = ""):
        """バケット内のファイル一覧を取得。"""
        if prefix:
            response = self.client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=prefix
            )
        else:
            response = self.client.list_objects_v2(Bucket=self.bucket_name)
        return response.get("Contents", [])

    def get_object(self, file_name: str):
        """指定したオブジェクトを取得。"""
        try:
            return self.client.get_object(
                Bucket=self.bucket_name, Key=file_name
            )
        except Exception as e:
            print(f"エラーが発生しました: {e}")
            return None

    def download_file(self, r2_key: str, local_path: Optional[str] = None):
        """
        指定したオブジェクトをローカルにダウンロード。

        Args:
            r2_key: R2 上のオブジェクトキー
            local_path: 保存先パス（省略時は r2_key のファイル名でカレントに保存）

        Returns:
            str: 保存したローカルパス
        """
        dest = local_path or r2_key.split("/")[-1]
        self.client.download_file(self.bucket_name, r2_key, str(dest))
        return str(dest)

    def list_files_recursive(self, prefix: str = ""):
        """指定したプレフィックス以下のオブジェクトを全て取得。"""
        files = []
        paginator = self.client.get_paginator("list_objects_v2")
        try:
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if "Contents" in page:
                    files.extend(page["Contents"])
            return files
        except Exception as e:
            print(f"ファイル一覧取得エラー: {e}")
            return []

    def upload_file(
        self,
        r2_path: str,
        local_file_path: str,
        content_type: Optional[str] = None,
    ):
        """ローカルファイルを R2 にアップロード。"""
        extra = {}
        if content_type:
            extra["ContentType"] = content_type
        elif r2_path.endswith(".parquet"):
            extra["ContentType"] = "application/vnd.apache.parquet"
        elif r2_path.endswith(".mp4"):
            extra["ContentType"] = "video/mp4"
        elif r2_path.endswith(".webm"):
            extra["ContentType"] = "video/webm"
        elif r2_path.endswith(".webp"):
            extra["ContentType"] = "image/webp"
        elif r2_path.endswith((".jpg", ".jpeg")):
            extra["ContentType"] = "image/jpeg"
        elif r2_path.endswith(".png"):
            extra["ContentType"] = "image/png"
        self.client.upload_file(
            local_file_path,
            self.bucket_name,
            r2_path,
            ExtraArgs=extra if extra else None,
        )

    def update_content_type(self, r2_path: str):
        """指定したオブジェクトの ContentType を拡張子から更新。"""
        content_type = None
        if r2_path.endswith(".parquet"):
            content_type = "application/vnd.apache.parquet"
        elif r2_path.endswith(".mp4"):
            content_type = "video/mp4"
        elif r2_path.endswith(".webm"):
            content_type = "video/webm"
        elif r2_path.endswith(".webp"):
            content_type = "image/webp"
        elif r2_path.endswith((".jpg", ".jpeg")):
            content_type = "image/jpeg"
        elif r2_path.endswith(".png"):
            content_type = "image/png"
        if content_type:
            copy_source = {"Bucket": self.bucket_name, "Key": r2_path}
            self.client.copy_object(
                CopySource=copy_source,
                Bucket=self.bucket_name,
                Key=r2_path,
                ContentType=content_type,
                MetadataDirective="REPLACE",
            )
            print(f"ContentType updated to {content_type}")
        else:
            print("No appropriate content type found for this file extension")

    def upload_byte_file(self, file_content: bytes, r2_path: str):
        """バイト列をそのまま R2 にアップロード。"""
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=r2_path,
            Body=file_content,
        )

    def delete_directory(self, prefix: str) -> bool:
        """指定したプレフィックス以下のオブジェクトを全て削除。"""
        print(f"ディレクトリ削除開始: {prefix}")
        try:
            objects_to_delete = self.list_files_recursive(prefix)
            if not objects_to_delete:
                print(f"削除対象のオブジェクトが見つかりません: {prefix}")
                return True
            keys_to_delete = [obj["Key"] for obj in objects_to_delete]
            batch_size = 1000
            for i in range(0, len(keys_to_delete), batch_size):
                batch = keys_to_delete[i : i + batch_size]
                objects = [{"Key": key} for key in batch]
                response = self.client.delete_objects(
                    Bucket=self.bucket_name, Delete={"Objects": objects}
                )
                deleted_count = len(response.get("Deleted", []))
                error_count = len(response.get("Errors", []))
                print(f"削除完了: {deleted_count}件, エラー: {error_count}件")
                if error_count > 0:
                    print("削除エラーが発生しました")
                    return False
            print(f"ディレクトリ削除完了: {prefix}")
            return True
        except Exception as e:
            print(f"ディレクトリ削除エラー: {e}")
            return False
