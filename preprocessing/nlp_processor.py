import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer

# Download necessary NLTK data (safe to run multiple times)
try:
    nltk.data.find('tokenizers/punkt')
    nltk.data.find('corpora/stopwords')
    nltk.data.find('corpora/wordnet')
    nltk.data.find('corpora/omw-1.4')
except (LookupError, OSError):
    try:
        nltk.download('punkt')
        nltk.download('stopwords')
        nltk.download('wordnet')
        nltk.download('omw-1.4')
    except Exception as e:
        print(f"Warning: NLTK download failed: {e}. using fallbacks.")

def get_stopwords_list():
    try:
        return set(stopwords.words('english'))
    except Exception:
        return set() # Fallback to empty

def tokenize_text(text):
    try:
        return word_tokenize(text)
    except Exception:
        return text.split() # Fallback to simple split

def remove_stopwords(tokens):
    stop_words = get_stopwords_list()
    return [word for word in tokens if word not in stop_words]

def lemmatize_tokens(tokens):
    try:
        lemmatizer = WordNetLemmatizer()
        return [lemmatizer.lemmatize(word) for word in tokens]
    except Exception:
        return tokens # Fallback: return as is

def process_nlp(text):
    """Full NLP pipeline: Tokenize -> Remove Stopwords -> Lemmatize"""
    tokens = tokenize_text(text)
    tokens = remove_stopwords(tokens)
    tokens = lemmatize_tokens(tokens)
    return tokens
