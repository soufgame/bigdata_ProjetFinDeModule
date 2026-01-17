import os

DIR_PATH = r"c:\Users\saade\Documents\bigdata_ProjetFinDeModule\kafka project"

print(f"Listing: {DIR_PATH}")
try:
    if os.path.exists(DIR_PATH):
        files = os.listdir(DIR_PATH)
        for f in files:
            print(f"File: {repr(f)}")
            if "ai_vs_human" in f:
                print(f"  -> Found potential match: {os.path.join(DIR_PATH, f)}")
    else:
        print("Directory not found!")
except Exception as e:
    print(f"Error: {e}")
