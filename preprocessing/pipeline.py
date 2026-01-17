import pandas as pd
import json
import os
import traceback
from cleaner import clean_text
from normalizer import normalize_text
from nlp_processor import process_nlp

def preprocess_row(text):
    if not isinstance(text, str):
        return []
    
    cleaned = clean_text(text)
    normalized = normalize_text(cleaned)
    tokens = process_nlp(normalized)
    return tokens

def main(input_path, output_path):
    print(f"Loading data from {input_path}...")
    
    try:
        # Try loading as CSV
        if input_path.endswith('.csv'):
            df = pd.read_csv(input_path)
            # Find the text column - assuming 'article', 'text', 'content' or similar. 
            # For now let's inspect columns or assume a default if not found.
            text_col = None
            possible_cols = ['article', 'text', 'content', 'body', 'News'] # Added 'News' based on common datasets
            
            for col in df.columns:
                if col in possible_cols:
                    text_col = col
                    break
            
            if not text_col:
                # Fallback: use the first object column or string column
                print(f"Columns found: {df.columns}")
                print("Could not identify text column automatically. Using the first column.")
                text_col = df.columns[0]

            print(f"Processing column: {text_col}")
            
            # Apply preprocessing
            # We will create a new column 'processed_tokens'
            df['processed_tokens'] = df[text_col].apply(preprocess_row)
            
            print(f"Saving processed data to {output_path}...")
            # Save as CSV or Parquet. Parquet is better for lists but CSV is requested/simpler for debugging.
            # If CSV, lists will be stringified.
            df.to_csv(output_path, index=False)
            print("Done.")

    except Exception as e:
        print(f"Error processing file: {e}")
        traceback.print_exc()

import sys

if __name__ == "__main__":
    # Default paths for testing
    # Robust path finding
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # goes to bigdata_ProjetFinDeModule
    
    if len(sys.argv) >= 3:
        INPUT_FILE = sys.argv[1]
        OUTPUT_FILE = sys.argv[2]
    else:
        INPUT_FILE = os.path.join(base_dir, "kafka project", "ai_vs_human_news.csv")
        OUTPUT_FILE = os.path.join(base_dir, "preprocessing", "processed_news.csv")
    
    print(f"Resolved Input Path: {INPUT_FILE}")
    
    if not os.path.exists(INPUT_FILE):
        print("File still not found. Listing base_dir/kafka project:")
        try:
             print(os.listdir(os.path.join(base_dir, "kafka project")))
        except Exception as e:
             print(f"Error listing dir: {e}")
        # Try absolute path based on known location
        INPUT_FILE = r"c:\Users\saade\Documents\bigdata_ProjetFinDeModule\kafka project\ai_vs_human_news.csv"

    main(INPUT_FILE, OUTPUT_FILE)
