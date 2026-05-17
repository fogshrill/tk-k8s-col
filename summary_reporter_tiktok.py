import glob
import json
import csv
import os
import subprocess
from pathlib import Path

def main():
    print("📊 Compiling Master TikTok Session Ledger...")
    report_files = glob.glob("NodeReport_*.json")
    
    if not report_files:
        print("⚠️ No Node Reports found!")
        return

    master_csv = "Master_Upload_Ledger_TikTok.csv"
    file_exists = Path(master_csv).exists()

    with open(master_csv, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["Timestamp", "Node", "Batch_Folder", "Primary_Assigned_Acc", "Final_Uploaded_Acc", "Status", "Files_Count"])

        for rep_file in report_files:
            try:
                with open(rep_file, 'r') as rf:
                    data = json.load(rf)
                    writer.writerow([
                        data.get("timestamp", ""), data.get("node", ""), data.get("batch_folder", ""),
                        data.get("primary_assigned_acc", ""), data.get("final_uploaded_acc", ""),
                        data.get("status", ""), data.get("files_count", 0)
                    ])
            except Exception as e:
                print(f"❌ Error reading {rep_file}: {e}")

    print(f"🎉 Master Ledger '{master_csv}' updated!")

    if os.environ.get("GITHUB_ACTIONS"):
        try:
            subprocess.run(["git", "config", "--global", "user.name", "github-actions"], check=True)
            subprocess.run(["git", "config", "--global", "user.email", "github-actions@github.com"], check=True)
            subprocess.run(["git", "add", master_csv], check=True)
            subprocess.run(["git", "commit", "-m", "📊 Update TikTok Master Ledger [skip ci]"], check=True)
            subprocess.run(["git", "push"], check=True)
            print("✅ Master Ledger pushed to GitHub repository!")
        except Exception as e:
            print(f"⚠️ Git push failed: {e}")

if __name__ == "__main__":
    main()