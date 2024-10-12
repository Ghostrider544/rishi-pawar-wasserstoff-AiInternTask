import os
import pdfplumber
import logging

def read_pdf(file_path):
    """Read and extract text from a PDF file."""
    try:
        with pdfplumber.open(file_path) as pdf:
            text = ''
            for page in pdf.pages:
                text += page.extract_text() + '\n'
        return text.strip()
    except Exception as e:
        logging.error(f"Error reading {file_path}: {e}")
        return None
from pymongo import MongoClient

class MongoDBClient:
    def __init__(self, db_name='pdf_database'):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client[db_name]
        self.collection = self.db['documents']

    def insert_document(self, document):
        """Insert a new document metadata entry."""
        self.collection.insert_one(document)

    def update_document(self, document_id, updates):
        """Update an existing document with summaries and keywords."""
        self.collection.update_one({"_id": document_id}, {"$set": updates})

from nltk.tokenize import sent_tokenize, word_tokenize
from collections import Counter

def summarize(text, num_sentences=3):
    """Generate a summary from the given text."""
    sentences = sent_tokenize(text)
    word_freq = Counter(word_tokenize(text.lower()))
    ranked_sentences = sorted(sentences, key=lambda s: sum(word_freq[word] for word in word_tokenize(s.lower())), reverse=True)
    return ' '.join(ranked_sentences[:num_sentences])

def extract_keywords(text, num_keywords=5):
    """Extract domain-specific keywords from the text."""
    words = word_tokenize(text.lower())
    word_freq = Counter(words)
    return [word for word, _ in word_freq.most_common(num_keywords)]

import os
import logging
from concurrent.futures import ThreadPoolExecutor
from pdf_processor import read_pdf
from mongo_client import MongoDBClient
from utils import summarize, extract_keywords

# Setup logging
logging.basicConfig(filename='logs/pipeline.log', level=logging.INFO)

def process_pdf(file_path, mongo_client):
    """Process a single PDF file."""
    text = read_pdf(file_path)
    if text is None:
        return  # Skip if there's an error

    summary = summarize(text)
    keywords = extract_keywords(text)

    document = {
        'file_name': os.path.basename(file_path),
        'path': file_path,
        'size': os.path.getsize(file_path),
        'summary': summary,
        'keywords': keywords
    }

    # Insert initial document metadata
    document_id = mongo_client.insert_document(document)

    # Update with summary and keywords
    mongo_client.update_document(document_id, {
        'summary': summary,
        'keywords': keywords
    })

def main(folder_path):
    mongo_client = MongoDBClient()
    
    with ThreadPoolExecutor() as executor:
        for file_name in os.listdir(folder_path):
            if file_name.endswith('.pdf'):
                file_path = os.path.join(folder_path, file_name)
                executor.submit(process_pdf, file_path, mongo_client)

if __name__ == '__main__':
    folder_path = 'test_documents'  # Path to your PDF folder
    main(folder_path)

import time
import psutil

def process_pdf(file_path, mongo_client):
    start_time = time.time()
    # existing logic...
    end_time = time.time()
    logging.info(f"Processed {file_path} in {end_time - start_time} seconds. Memory usage: {psutil.Process().memory_info().rss / 1024 ** 2} MB")
