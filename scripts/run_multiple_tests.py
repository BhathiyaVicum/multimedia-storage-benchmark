import os
import time
import shutil

def run_multiple_simple(num_runs=5):
    """Run experiments multiple times for statistical significance"""
    
    print(f"🎯 Starting {num_runs} experiment runs for statistical significance")
    print(f"📊 Expected total data points: {num_runs * 24}")
    
    # Clean up any existing run files
    for i in range(1, num_runs + 1):
        run_file = f"../results/run{i}.csv"
        if os.path.exists(run_file):
            os.remove(run_file)
            print(f"🧹 Removed existing: run{i}.csv")
    
    for i in range(1, num_runs + 1):
        print(f"\n🚀 STARTING RUN {i}/{num_runs}")
        print("=" * 50)
        
        # Import and run cleanup
        from cleanup_storage import cleanup_all
        cleanup_all()
        
        # Import and run experiments
        from run_experiments import main
        print("🔬 Running experiments...")
        main()  # This will run the experiments
        
        # Rename results - with better error handling
        source_file = "../results/experiment_results.csv"
        target_file = f"../results/run{i}.csv"
        
        if os.path.exists(source_file):
            # Remove target if it exists
            if os.path.exists(target_file):
                os.remove(target_file)
            
            # Rename the file
            os.rename(source_file, target_file)
            print(f"✅ Results saved: run{i}.csv")
            
            # Show how many data points got
            import pandas as pd
            try:
                df = pd.read_csv(target_file)
                print(f"📊 Data points collected: {len(df)}")
                
                # Show success rates for this run
                success_rate = df['upload_success'].mean() * 100
                print(f"🎯 Success rate: {success_rate:.1f}%")
                
                # Show performance summary
                avg_upload = df['upload_speed_mb_sec'].mean()
                avg_download = df['retrieval_speed_mb_sec'].mean()
                print(f"⚡ Avg upload: {avg_upload:.1f} MB/s, download: {avg_download:.1f} MB/s")
            except Exception as e:
                print(f"📊 Could not read CSV file: {e}")
        else:
            print("❌ No results file generated!")
        
        # Short wait between runs
        if i < num_runs:
            print("⏳ Waiting 10 seconds before next run...")
            time.sleep(10)
    
    print(f"\n🎉 ALL {num_runs} RUNS COMPLETED!")
    print(f"📈 Total data points: ~{num_runs * 24}")

if __name__ == "__main__":
    run_multiple_simple(5)