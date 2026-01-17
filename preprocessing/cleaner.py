import re

def remove_urls(text):
    """Removes URLs from text."""
    url_pattern = re.compile(r'https?://\S+|www\.\S+')
    return url_pattern.sub(r'', text)

def remove_html_tags(text):
    """Removes HTML tags from text."""
    html_pattern = re.compile(r'<.*?>')
    return html_pattern.sub(r'', text)

def remove_emojis(text):
    """Removes emojis from text (basic implementation using regex for non-ascii)."""
    return text.encode('ascii', 'ignore').decode('ascii')

def clean_text(text):
    """Applies all cleaning functions to the text."""
    if not isinstance(text, str):
        return ""
    text = remove_urls(text)
    text = remove_html_tags(text)
    text = remove_emojis(text)
    return text.strip()
