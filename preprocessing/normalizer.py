import re
import string

def to_lowercase(text):
    return text.lower()

def remove_punctuation(text):
    return text.translate(str.maketrans('', '', string.punctuation))

def remove_numbers(text):
    """Removes numbers from text."""
    return re.sub(r'\d+', '', text)

def remove_extra_whitespace(text):
    return " ".join(text.split())

def normalize_text(text):
    """Applies normalization steps."""
    if not isinstance(text, str):
        return ""
    text = to_lowercase(text)
    text = remove_punctuation(text)
    text = remove_numbers(text)
    text = remove_extra_whitespace(text)
    return text
