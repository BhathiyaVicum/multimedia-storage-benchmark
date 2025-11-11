from hdfs import InsecureClient

def test_hdfs():
    try:
        client = InsecureClient('http://localhost:9870', user='root')
        print("✅ HDFS Connection Successful!")
        
        # List root directory
        files = client.list('/')
        print(f"✅ HDFS Root Contents: {files}")
        return True
    except Exception as e:
        print(f"❌ HDFS Error: {e}")
        return False

if __name__ == "__main__":
    test_hdfs()