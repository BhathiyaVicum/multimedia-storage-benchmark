import psutil
import os

def get_system_stats():
    """Get current system resource usage"""
    return {
        'cpu_percent': psutil.cpu_percent(),
        'memory_percent': psutil.virtual_memory().percent,
        'disk_usage': psutil.disk_usage('/').percent
    }

def format_file_size(size_bytes):
    """Convert bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"

def scan_datasets_folder():
    """Scan the datasets folder and return all files with their categories"""
    files_by_category = {
        'small': [],
        'medium': [], 
        'large': []
    }
    
    for category in files_by_category.keys():
        category_path = f"./datasets/{category}"
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
    