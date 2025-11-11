from hdfs_client import HDFSClient
from minio_client import MinioClient
from mongodb_client import MongoDBClient

def cleanup_all():
    """Clean up all storage systems before experiments"""
    print("🧹 Cleaning storage systems...")
    
    try:
        # Initialize all storage clients
        hdfs = HDFSClient()
        minio = MinioClient()
        mongo = MongoDBClient()
        
        print("🔄 Cleaning HDFS...")
        hdfs.cleanup()
        
        print("🔄 Cleaning MinIO...")
        minio.cleanup()
        
        print("🔄 Cleaning MongoDB...")
        mongo.cleanup()
        
        print("✅ All storage systems cleaned!")
        return True
        
    except Exception as e:
        print(f"❌ Cleanup error: {e}")
        return False

if __name__ == "__main__":
    cleanup_all()