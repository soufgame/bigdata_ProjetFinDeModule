import pandas as pd
import ast
import re

# Load the data
df = pd.read_csv('processed_news.csv')

print("=" * 60)
print("DATA QUALITY ANALYSIS FOR processed_news.csv")
print("=" * 60)

# 1. Basic Statistics
print("\n1. BASIC STATISTICS:")
print(f"   - Total rows: {len(df)}")
print(f"   - Total columns: {len(df.columns)}")
print(f"   - Columns: {df.columns.tolist()}")

# 2. Missing Values
print("\n2. MISSING VALUES:")
null_counts = df.isnull().sum()
null_pct = (null_counts / len(df) * 100).round(2)
missing_df = pd.DataFrame({
    'Column': null_counts.index,
    'Missing Count': null_counts.values,
    'Missing %': null_pct.values
})
print(missing_df[missing_df['Missing Count'] > 0].to_string(index=False))

# 3. Empty Strings
print("\n3. EMPTY STRINGS:")
empty_found = False
for col in df.columns:
    empty_count = (df[col].astype(str).str.strip() == '').sum()
    if empty_count > 0:
        empty_found = True
        print(f"   - {col}: {empty_count} empty strings")
if not empty_found:
    print("   ✓ No empty strings found")

# 4. Duplicates
print("\n4. DUPLICATE ANALYSIS:")
print(f"   - Duplicate rows: {df.duplicated().sum()}")
print(f"   - Duplicate URLs: {df['url'].duplicated().sum()}")
print(f"   - Duplicate titles: {df['title'].duplicated().sum()}")

# 5. URL Uniqueness
print("\n5. URL UNIQUENESS:")
print(f"   - Unique URLs: {df['url'].nunique()} out of {len(df)}")

# 6. Date Analysis
print("\n6. DATE ANALYSIS:")
try:
    df['published_at_parsed'] = pd.to_datetime(df['published_at'])
    print(f"   - Date range: {df['published_at_parsed'].min()} to {df['published_at_parsed'].max()}")
    print(f"   - Invalid dates: 0")
except Exception as e:
    print(f"   - Error parsing dates: {e}")

# 7. Processed Tokens Analysis
print("\n7. PROCESSED TOKENS ANALYSIS:")
try:
    token_lengths = []
    invalid_tokens = 0
    for idx, tokens_str in enumerate(df['processed_tokens'].head(10)):
        try:
            tokens = ast.literal_eval(tokens_str)
            if isinstance(tokens, list):
                token_lengths.append(len(tokens))
            else:
                invalid_tokens += 1
        except:
            invalid_tokens += 1
    
    if token_lengths:
        print(f"   - Sample token counts (first 10): {token_lengths}")
        print(f"   - Invalid token formats in sample: {invalid_tokens}")
    
    # Check for empty token lists
    empty_tokens = 0
    for tokens_str in df['processed_tokens']:
        try:
            tokens = ast.literal_eval(tokens_str)
            if isinstance(tokens, list) and len(tokens) == 0:
                empty_tokens += 1
        except:
            pass
    print(f"   - Empty token lists: {empty_tokens}")
except Exception as e:
    print(f"   - Error analyzing tokens: {e}")

# 8. Content Quality Checks
print("\n8. CONTENT QUALITY:")
# Check for HTML tags in content
html_pattern = re.compile(r'<[^>]+>')
html_in_content = df['content'].astype(str).str.contains(html_pattern).sum()
html_in_description = df['description'].astype(str).str.contains(html_pattern).sum()
html_in_title = df['title'].astype(str).str.contains(html_pattern).sum()

print(f"   - HTML tags in title: {html_in_title}")
print(f"   - HTML tags in description: {html_in_description}")
print(f"   - HTML tags in content: {html_in_content}")

# Check for special characters/encoding issues
print("\n9. CHARACTER ENCODING ISSUES:")
encoding_issues = 0
for col in ['title', 'description', 'content']:
    issues = df[col].astype(str).str.contains(r'\\x[0-9a-fA-F]{2}|\\u[0-9a-fA-F]{4}', regex=True).sum()
    if issues > 0:
        print(f"   - {col}: {issues} rows with potential encoding issues")
        encoding_issues += issues
if encoding_issues == 0:
    print("   ✓ No obvious encoding issues detected")

# 10. Data Consistency
print("\n10. DATA CONSISTENCY:")
print(f"   - All sources: {df['source'].unique()}")
print(f"   - Keywords format check (first 3):")
for i in range(min(3, len(df))):
    print(f"     Row {i}: {df.loc[i, 'keywords']}")

print("\n" + "=" * 60)
print("SUMMARY:")
print("=" * 60)

issues = []
if null_counts.sum() > 0:
    issues.append(f"Missing values found in {(null_counts > 0).sum()} columns")
if df.duplicated().sum() > 0:
    issues.append(f"{df.duplicated().sum()} duplicate rows")
if html_in_content > 0 or html_in_description > 0 or html_in_title > 0:
    issues.append("HTML tags found in text fields")
if encoding_issues > 0:
    issues.append("Character encoding issues detected")

if issues:
    print("\n⚠️  ISSUES FOUND:")
    for i, issue in enumerate(issues, 1):
        print(f"   {i}. {issue}")
else:
    print("\n✓ Data appears to be clean!")

print("\n" + "=" * 60)
