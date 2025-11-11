import time
import csv
import os
from hdfs_client import HDFSClient
from minio_client import MinioClient
from mongodb_client import MongoDBClient
from utils import get_system_stats, format_file_size

def scan_datasets_folder():
    """Scan the datasets folder and return all files with their categories"""
    files_by_category = {
        'small': [],
        'medium': [], 
        'large': []
    }
    
    for category in files_by_category.keys():
        category_path = f"../datasets/{category}"  # FIXED PATH
        if os.path.exists(category_path):
            for file in os.listdir(category_path):
                file_path = os.path.join(category_path, file)
                if os.path.isfile(file_path):
                    files_by_category[category].append({
                        'path': file_path,
                        'name': file,
                        'size': os.path.getsize(file_path)
                    })
    
    return files_by_category

def run_comprehensive_experiments():
    """Run comprehensive tests across all storage systems and file categories"""
    
    # Initialize storage clients
    hdfs = HDFSClient()
    minio = MinioClient()
    mongo = MongoDBClient()
    
    systems = [hdfs, minio, mongo]
    results = []
    
    # Scan for available files
    dataset_files = scan_datasets_folder()
    
    print("=== Starting Comprehensive Multimedia Storage Experiments ===")
    print("Found files:")
    for category, files in dataset_files.items():
        print(f"  {category}: {len(files)} files")
    
    # Run tests for each category
    for category, files in dataset_files.items():
        if not files:
            print(f"No files found in {category} category, skipping...")
            continue
            
        # Test with first 3 files of each category
        test_files = files[:min(3, len(files))]
        
        for file_info in test_files:
            file_path = file_info['path']
            file_name = file_info['name']
            file_size = file_info['size']
            
            for system in systems:
                system_name = system.get_name()
                
                print(f"\nTesting {system_name} | {category} | {file_name} ({format_file_size(file_size)})")
                
                # 1. UPLOAD TEST
                upload_start = time.time()
                upload_success = system.upload_file(file_path, f"{category}/{file_name}")
                upload_time = time.time() - upload_start
                upload_speed = file_size / upload_time / (1024**2) if upload_time > 0 else 0
                
                # Brief pause between operations
                time.sleep(2)
                
                # 2. RETRIEVAL TEST  
                download_path = f"../temp_downloads/{system_name}_{category}_{file_name}"  # FIXED PATH
                retrieval_start = time.time()
                retrieval_success = system.retrieve_file(f"{category}/{file_name}", download_path)
                retrieval_time = time.time() - retrieval_start
                retrieval_speed = file_size / retrieval_time / (1024**2) if retrieval_time > 0 else 0
                
                # 3. VERIFY INTEGRITY
                download_verified = False
                if retrieval_success and os.path.exists(download_path):
                    download_size = os.path.getsize(download_path)
                    download_verified = (download_size == file_size)
                
                # 4. RECORD RESULTS
                system_stats = get_system_stats()
                result = {
                    "storage_system": system_name,
                    "file_category": category,
                    "file_name": file_name,
                    "file_size_bytes": file_size,
                    "file_type": os.path.splitext(file_name)[1].lower(),
                    "upload_time_sec": round(upload_time, 4),
                    "upload_speed_mb_sec": round(upload_speed, 4),
                    "retrieval_time_sec": round(retrieval_time, 4),
                    "retrieval_speed_mb_sec": round(retrieval_speed, 4),
                    "upload_success": upload_success,
                    "retrieval_success": retrieval_success,
                    "download_verified": download_verified,
                    "cpu_usage": system_stats['cpu_percent'],
                    "memory_usage": system_stats['memory_percent'],
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                }
                results.append(result)
                
                # Cleanup downloaded file
                if os.path.exists(download_path):
                    os.remove(download_path)
                
                print(f"  Upload: {upload_time:.2f}s ({upload_speed:.2f} MB/s) | "
                      f"Retrieval: {retrieval_time:.2f}s ({retrieval_speed:.2f} MB/s) | "
                      f"Success: {upload_success & retrieval_success}")
    
    return results

def save_and_analyze_results(results):
    """Save results to CSV and provide summary"""
    if not results:
        print("No results to save!")
        return
    
    # Save to CSV
    csv_path = "../results/experiment_results.csv"
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)
    
    print(f"\n=== Results Summary ===")
    print(f"Results saved to: {csv_path}")
    print(f"Total test runs: {len(results)}")
    
    # Summary by storage system
    systems = set(r['storage_system'] for r in results)
    for system in systems:
        system_results = [r for r in results if r['storage_system'] == system]
        
        print(f"\n{system}:")
        print(f"  Test runs: {len(system_results)}")
        print(f"  Success rate: {mean([1 if r['upload_success'] and r['retrieval_success'] else 0 for r in system_results])*100:.1f}%")
        print(f"  Avg upload speed: {mean([r['upload_speed_mb_sec'] for r in system_results]):.2f} MB/s")
        print(f"  Avg retrieval speed: {mean([r['retrieval_speed_mb_sec'] for r in system_results]):.2f} MB/s")
    
    # Summary by file category
    categories = set(r['file_category'] for r in results)
    print(f"\nPerformance by file category:")
    for category in categories:
        category_results = [r for r in results if r['file_category'] == category]
        avg_size = mean([r['file_size_bytes'] for r in category_results]) / (1024**2)
        print(f"  {category}: {len(category_results)} tests, avg size: {avg_size:.1f} MB")

def mean(values):
    """Calculate mean of a list"""
    return sum(values) / len(values) if values else 0

def main():
    """Main execution function"""
    # Create necessary directories
    os.makedirs("../temp_downloads", exist_ok=True)
    os.makedirs("../results", exist_ok=True)
    
    # Check if datasets exist
    if not os.path.exists("../datasets"):
        print("ERROR: 'datasets' folder not found!")
        print("Please create 'datasets' folder in the project root with 'small/', 'medium/', 'large/' subfolders")
        return
    
    # Run experiments
    results = run_comprehensive_experiments()
    
    # Save and analyze results
    save_and_analyze_results(results)

if __name__ == "__main__":
    main()