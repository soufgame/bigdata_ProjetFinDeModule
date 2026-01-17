import os
import pandas as pd

FILE_PATH = r"c:\Users\saade\Documents\bigdata_ProjetFinDeModule\kafka project\ai_vs_human_news.csv"

print(f"Checking file: {FILE_PATH}")
if os.path.exists(FILE_PATH):
    print("File exists.")
    print(f"Size: {os.path.getsize(FILE_PATH)} bytes")
    try:
        with open(FILE_PATH, 'r', encoding='utf-8') as f:
            print(f"First line: {f.readline()}")
        
        print("Attempting pandas read...")
        df = pd.read_csv(FILE_PATH)
        print(f"Pandas read success. Shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
    except Exception as e:
        print(f"Error reading file: {e}")
else:
    print("File does NOT exist.")
