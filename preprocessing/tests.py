import unittest
from cleaner import clean_text
from normalizer import normalize_text
from nlp_processor import process_nlp

class TestPreprocessing(unittest.TestCase):
    
    def test_cleaner(self):
        text = "Hello World! https://example.com 😊"
        cleaned = clean_text(text)
        # cleaner removes URLs and emojis (non-ascii)
        self.assertEqual(cleaned, "Hello World!")
        
    def test_normalizer(self):
        text = "Hello World! 123"
        normalized = normalize_text(text)
        # normalizer: lower, remove punct, remove numbers
        self.assertEqual(normalized, "hello world")
        
    def test_nlp(self):
        text = "cats running"
        # process_nlp: tokenize, remove stop, lemmatize
        # cats -> cat, running -> running (depends on POS, default noun) -> running
        # actually running -> run if verb. Lemmatizer needs pos. Default is noun.
        # cats -> cat.
        tokens = process_nlp(text)
        self.assertIn("cat", tokens)
        self.assertIn("running", tokens) 

if __name__ == '__main__':
    unittest.main()
