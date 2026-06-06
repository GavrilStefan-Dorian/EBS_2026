import subprocess
import os
import shutil
import re

def generate_data(percentage, output_dir):
    """
    Modifies the Config.java to enforce a specific percentage of '=' operators
    for 'company', compiles the generator, and runs it to produce publications
    and subscriptions.
    """
    print(f"\n--- Generating data for {percentage}% equality ---")
    config_path = "../../generator/src/Config.java"
    
    # Backup original Config.java to restore later
    backup_path = config_path + ".bak"
    if not os.path.exists(backup_path):
        shutil.copy2(config_path, backup_path)
    
    try:
        with open(config_path, "r") as f:
            content = f.read()
            
        # Replace the percentage value for "company"
        # Specifically targeting: "company", 70.0 (or similar)
        content = re.sub(r'("company",\s*)\d+\.\d+', rf'\g<1>{float(percentage)}', content)
        
        with open(config_path, "w") as f:
            f.write(content)
            
        # Compile Java
        out_dir = "../../generator/out"
        os.makedirs(out_dir, exist_ok=True)
        print(f"Compiling generator...")
        subprocess.run(["javac", "-d", out_dir, "../../generator/src/Config.java", "../../generator/src/Generator.java", "../../generator/src/Main.java", "../../generator/src/BenchmarkRunner.java"], check=True)
        
        # Run generator
        print(f"Running generator to {output_dir}...")
        # Main args: publications, subscriptions, parallelism, outputDir, seed
        subprocess.run(["java", "-cp", out_dir, "Main", "3600", "10000", "4", output_dir, "42"], check=True)
    finally:
        # Restore Config.java
        if os.path.exists(backup_path):
            shutil.move(backup_path, config_path)

if __name__ == "__main__":
    # Go to script directory to ensure relative paths work
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    generate_data(100, "data_100")
    generate_data(25, "data_25")
    print("Data generation complete. Files are in 'data_100' and 'data_25' directories.")
