import pandas as pd
import os
import glob
from datetime import datetime

def combine_runs():
    """Combine all experiment runs into one master dataset"""
    
    print("🔗 COMBINING EXPERIMENT RUNS")
    print("=" * 60)
    
    all_data = []
    runs_found = []
    
    for i in range(1, 6):  # Check for runs 1-5
        file_path = f"../results/run{i}.csv"
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path)
                df['run_id'] = i 
                all_data.append(df)
                runs_found.append(i)
                print(f"✅ RUN {i}: {len(df):2d} data points")
                
                # Show quick stats for this run
                success_rate = df['upload_success'].mean() * 100
                avg_upload = df['upload_speed_mb_sec'].mean()
                avg_download = df['retrieval_speed_mb_sec'].mean()
                print(f"   📈 Success: {success_rate:5.1f}% | ⬆️ Upload: {avg_upload:5.1f} MB/s | ⬇️ Download: {avg_download:5.1f} MB/s")
                
            except Exception as e:
                print(f"❌ Error reading run {i}: {e}")
        else:
            print(f"❌ Run {i} not found")
    
    if all_data:
        # Combine all data
        combined = pd.concat(all_data, ignore_index=True)
        
        # Save combined dataset
        combined_path = '../results/combined_results.csv'
        combined.to_csv(combined_path, index=False)
        
        print(f"\n" + "=" * 60)
        print("🎯 COMBINED DATASET SUMMARY")
        print("=" * 60)
        print(f"📁 File: {combined_path}")
        print(f"📊 Total data points: {len(combined)}")
        print(f"🔢 Runs combined: {runs_found}")
        print(f"📅 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Generate detailed summary statistics
        print(f"\n📈 PERFORMANCE OVERVIEW")
        print("-" * 40)
        print(f"   Total experiments: {len(combined)}")
        print(f"   Unique test runs: {combined['run_id'].nunique()}")
        print(f"   Overall success rate: {combined['upload_success'].mean() * 100:.1f}%")
        
        # Summary by storage system
        print(f"\n🏗️ STORAGE SYSTEM PERFORMANCE")
        print("-" * 40)
        systems = combined['storage_system'].unique()
        
        performance_data = []
        for system in systems:
            system_data = combined[combined['storage_system'] == system]
            upload_success = system_data['upload_success'].mean() * 100
            download_success = system_data['retrieval_success'].mean() * 100
            avg_upload_speed = system_data['upload_speed_mb_sec'].mean()
            avg_download_speed = system_data['retrieval_speed_mb_sec'].mean()
            test_count = len(system_data)
            
            performance_data.append({
                'System': system,
                'Tests': test_count,
                'Upload Success': upload_success,
                'Download Success': download_success,
                'Upload Speed': avg_upload_speed,
                'Download Speed': avg_download_speed
            })
            
            print(f"   {system:12} | Tests: {test_count:3d} | Success: {upload_success:5.1f}% ⬆️ {download_success:5.1f}% ⬇️")
            print(f"   {' ':12} | Speed:  {avg_upload_speed:5.1f} MB/s ⬆️ {avg_download_speed:5.1f} MB/s ⬇️")
            print(f"   {'-' * 50}")
        
        # Summary by file category
        print(f"\n📁 PERFORMANCE BY FILE CATEGORY")
        print("-" * 40)
        categories = combined['file_category'].unique()
        
        for category in categories:
            category_data = combined[combined['file_category'] == category]
            avg_size_mb = category_data['file_size_bytes'].mean() / (1024 * 1024)
            test_count = len(category_data)
            avg_speed = category_data['upload_speed_mb_sec'].mean()
            
            print(f"   {category:8} | Tests: {test_count:3d} | Avg Size: {avg_size_mb:6.1f} MB | Avg Speed: {avg_speed:5.1f} MB/s")
        
        # File type distribution
        print(f"\n📄 FILE TYPE DISTRIBUTION")
        print("-" * 40)
        file_types = combined['file_type'].value_counts().head(10)
        
        for file_type, count in file_types.items():
            percentage = (count / len(combined)) * 100
            print(f"   {file_type:8} | {count:3d} files ({percentage:5.1f}%)")
        
        # Performance trends across runs
        print(f"\n📈 PERFORMANCE CONSISTENCY ACROSS RUNS")
        print("-" * 40)
        run_performance = combined.groupby('run_id')['upload_speed_mb_sec'].mean()
        
        for run_id, avg_speed in run_performance.items():
            print(f"   Run {run_id} | Avg Speed: {avg_speed:6.1f} MB/s")
        
        # Save performance summary
        summary_path = '../results/analysis/performance_summary.txt'
        os.makedirs('../results/analysis', exist_ok=True)
        
        with open(summary_path, 'w') as f:
            f.write("Multimedia Storage Research - Performance Summary\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Total data points: {len(combined)}\n")
            f.write(f"Runs combined: {runs_found}\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("STORAGE SYSTEM PERFORMANCE:\n")
            f.write("-" * 30 + "\n")
            for perf in performance_data:
                f.write(f"{perf['System']}:\n")
                f.write(f"  Tests: {perf['Tests']}\n")
                f.write(f"  Success Rate: {perf['Upload Success']:.1f}% upload, {perf['Download Success']:.1f}% download\n")
                f.write(f"  Average Speed: {perf['Upload Speed']:.1f} MB/s upload, {perf['Download Speed']:.1f} MB/s download\n\n")
        
        print(f"\n💾 Summary saved to: {summary_path}")
        print(f"📊 Ready for analysis: python analyze_results.py")
            
    else:
        print("❌ No data files found to combine!")

def check_data_quality():
    """Check data quality and completeness"""
    print(f"\n🔍 DATA QUALITY CHECK")
    print("-" * 40)
    
    combined_path = '../results/combined_results.csv'
    if os.path.exists(combined_path):
        df = pd.read_csv(combined_path)
        
        print(f"✅ Combined dataset loaded: {len(df)} records")
        print(f"📅 Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        print(f"🔢 Unique runs: {df['run_id'].nunique()}")
        print(f"🏗️ Systems: {df['storage_system'].unique()}")
        print(f"📁 Categories: {df['file_category'].unique()}")
        
        # Check for missing values
        missing = df.isnull().sum().sum()
        print(f"❓ Missing values: {missing}")
        
        if missing > 0:
            print("⚠️  Warning: Dataset contains missing values")

if __name__ == "__main__":
    combine_runs()
    check_data_quality()