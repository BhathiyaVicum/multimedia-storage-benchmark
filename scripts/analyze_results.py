import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def analyze_results():
    """Analyze experiment results and generate charts"""
    
    # Read results
    results_path = "../results/combined_results.csv"
    if not os.path.exists(results_path):
        print("No combined results file found! Run combine_results.py first.")
        return
    
    df = pd.read_csv(results_path)
    
    print("=== Data Analysis ===")
    print(f"Total data points: {len(df)}")
    print(f"Runs included: {df['run_id'].unique()}")
    
    # Set up plotting style
    plt.style.use('seaborn-v0_8')
    sns.set_palette("Set2")
    
    # Create analysis directory
    os.makedirs("../results/analysis", exist_ok=True)
    
    # 1. Performance by Storage System
    plt.figure(figsize=(12, 6))
    
    plt.subplot(1, 2, 1)
    upload_speed = df.groupby('storage_system')['upload_speed_mb_sec'].mean()
    upload_speed.plot(kind='bar', color=['#ff6b6b', '#4ecdc4', '#45b7d1'])
    plt.title('Average Upload Speed by Storage System')
    plt.ylabel('Speed (MB/s)')
    plt.xticks(rotation=45)
    
    plt.subplot(1, 2, 2)
    download_speed = df.groupby('storage_system')['retrieval_speed_mb_sec'].mean()
    download_speed.plot(kind='bar', color=['#ff6b6b', '#4ecdc4', '#45b7d1'])
    plt.title('Average Download Speed by Storage System')
    plt.ylabel('Speed (MB/s)')
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    plt.savefig('../results/analysis/performance_by_system.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    # 2. Performance by File Category
    plt.figure(figsize=(10, 6))
    
    category_performance = df.groupby(['storage_system', 'file_category'])[['upload_speed_mb_sec', 'retrieval_speed_mb_sec']].mean().reset_index()
    
    plt.subplot(1, 2, 1)
    sns.barplot(data=category_performance, x='file_category', y='upload_speed_mb_sec', hue='storage_system')
    plt.title('Upload Speed by File Category')
    plt.xticks(rotation=45)
    
    plt.subplot(1, 2, 2)
    sns.barplot(data=category_performance, x='file_category', y='retrieval_speed_mb_sec', hue='storage_system')
    plt.title('Download Speed by File Category')
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    plt.savefig('../results/analysis/performance_by_category.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    # 3. Generate Summary Report
    print("\n=== PERFORMANCE SUMMARY ===")
    for system in df['storage_system'].unique():
        system_data = df[df['storage_system'] == system]
        print(f"\n{system}:")
        print(f"  Average Upload Speed: {system_data['upload_speed_mb_sec'].mean():.2f} MB/s")
        print(f"  Average Download Speed: {system_data['retrieval_speed_mb_sec'].mean():.2f} MB/s")
        print(f"  Upload Success Rate: {system_data['upload_success'].mean()*100:.1f}%")
        print(f"  Download Success Rate: {system_data['retrieval_success'].mean()*100:.1f}%")
    
    print("\nAnalysis complete! Check '../results/analysis/' folder for charts.")

if __name__ == "__main__":
    analyze_results()