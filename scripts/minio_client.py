from minio import Minio
from minio.error import S3Error

class MinioClient:
    def __init__(self):
        self.client = Minio(
            "localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
        self.bucket_name = "multimedia-test"
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)

    def get_name(self):
        return "MinIO"

    def upload_file(self, file_path, object_name):
        try:
            self.client.fput_object(
                self.bucket_name, 
                object_name, 
                file_path
            )
            return True
        except S3Error as e:
            print(f"MinIO Upload Error: {e}")
            return False

    def retrieve_file(self, object_name, download_path):
        try:
            self.client.fget_object(
                self.bucket_name, 
                object_name, 
                download_path
            )
            return True
        except S3Error as e:
            print(f"MinIO Retrieval Error: {e}")
            return False
    
    def cleanup(self):
        """Clean up MinIO bucket"""
        try:
            objects = self.client.list_objects(self.bucket_name, recursive=True)
            for obj in objects:
                self.client.remove_object(self.bucket_name, obj.object_name)
            print("MinIO cleaned")
        except Exception as e:
            print(f"MinIO cleanup: {e}")