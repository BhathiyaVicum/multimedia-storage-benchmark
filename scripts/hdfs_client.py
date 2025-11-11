import requests
import os
import time

class HDFSClient:
    def __init__(self):
        self.namenode_url = "http://localhost:9870/webhdfs/v1"
        self.base_path = "/multimedia"
        
    def get_name(self):
        return "HDFS"
    
    def upload_file(self, file_path, object_name):
        try:
            hdfs_path = f"{self.base_path}/{object_name}"
            
            # Step 1: Create file and get datanode redirect
            create_url = f"{self.namenode_url}{hdfs_path}?op=CREATE&overwrite=true&user.name=root"
            response = requests.put(create_url, allow_redirects=False)
            
            if response.status_code not in [307, 308]:
                return False
            
            # Step 2: Extract redirect URL and fix if needed
            redirect_url = response.headers['Location']
            
            # Fix: Replace 'datanode:9864' with 'localhost:9864'
            if 'datanode:9864' in redirect_url:
                redirect_url = redirect_url.replace('datanode:9864', 'localhost:9864')
            
            # Step 3: Upload file data
            with open(file_path, 'rb') as f:
                upload_response = requests.put(redirect_url, data=f, headers={'Content-Type': 'application/octet-stream'})
            
            return upload_response.status_code == 201
                
        except Exception as e:
            return False
    
    def retrieve_file(self, object_name, download_path):
        try:
            hdfs_path = f"{self.base_path}/{object_name}"
            
            # Step 1: Get file and get datanode redirect
            open_url = f"{self.namenode_url}{hdfs_path}?op=OPEN&user.name=root"
            response = requests.get(open_url, allow_redirects=False)
            
            if response.status_code not in [307, 308]:
                return False
            
            # Step 2: Extract redirect URL and fix if needed
            redirect_url = response.headers['Location']
            
            # Fix: Replace 'datanode:9864' with 'localhost:9864'
            if 'datanode:9864' in redirect_url:
                redirect_url = redirect_url.replace('datanode:9864', 'localhost:9864')
            
            # Step 3: Download file data
            download_response = requests.get(redirect_url)
            
            if download_response.status_code == 200:
                with open(download_path, 'wb') as f:
                    f.write(download_response.content)
                return True
            else:
                return False
                
        except Exception as e:
            return False
    
    def cleanup(self):
        try:
            delete_url = f"{self.namenode_url}{self.base_path}?op=DELETE&recursive=true&user.name=root"
            response = requests.delete(delete_url)
            if response.status_code == 200:
                print("✅ HDFS cleaned")
            else:
                print(f"ℹ️ HDFS cleanup: {response.status_code}")
        except Exception as e:
            print(f"ℹ️ HDFS cleanup: {e}")