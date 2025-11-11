from pymongo import MongoClient
from gridfs import GridFS
import os

class MongoDBClient:
    def __init__(self):
        self.client = MongoClient(
            'mongodb://admin:password@localhost:27017/',
            serverSelectionTimeoutMS=5000
        )
        self.db = self.client['multimedia_db']
        self.fs = GridFS(self.db)
        self.collection = self.db['files_metadata']

    def get_name(self):
        return "MongoDB"

    def upload_file(self, file_path, object_name):
        try:
            with open(file_path, 'rb') as file_data:
                # Store file in GridFS
                file_id = self.fs.put(
                    file_data, 
                    filename=object_name,
                    metadata={
                        'original_name': object_name,
                        'file_size': os.path.getsize(file_path)
                    }
                )
                
                # Store metadata in collection
                self.collection.insert_one({
                    'filename': object_name,
                    'gridfs_id': file_id,
                    'uploaded_at': 'now'
                })
                
            return True
        except Exception as e:
            print(f"MongoDB Upload Error: {e}")
            return False

    def retrieve_file(self, object_name, download_path):
        try:
            # Find the file metadata
            file_meta = self.collection.find_one({'filename': object_name})
            if not file_meta:
                return False
                
            # Get file from GridFS
            grid_out = self.fs.get(file_meta['gridfs_id'])
            
            with open(download_path, 'wb') as file_data:
                file_data.write(grid_out.read())
                
            return True
        except Exception as e:
            print(f"MongoDB Retrieval Error: {e}")
            return False
    
    def cleanup(self):
        """Clean up MongoDB collections"""
        try:
            self.db['fs.files'].drop()
            self.db['fs.chunks'].drop()
            self.collection.drop()
            print("MongoDB cleaned")
        except Exception as e:
            print(f"MongoDB cleanup: {e}")