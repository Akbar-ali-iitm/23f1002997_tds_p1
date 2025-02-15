# Phase B: LLM-based Automation Agent for DataWorks Solutions

import os
import requests
import sqlite3
import duckdb
import markdown
from PIL import Image
from flask import Flask, request, jsonify
import pandas as pd
import subprocess

# Flask app instance for B10
app = Flask(__name__)

def B12(filepath):
    """Check if a given file path is within the /data directory."""
    if not filepath.startswith('/data'):
        raise PermissionError("Access outside /data is not allowed.")
    return True

def B2(filepath):
    """Ensure data is never deleted anywhere on the file system."""
    if os.path.exists(filepath):
        raise PermissionError("File deletion is not allowed.")
    return True

def B3(url, save_path):
    """Fetch data from an API and save it."""
    try:
        B12(save_path)
        response = requests.get(url)
        response.raise_for_status()
        with open(save_path, 'w') as file:
            file.write(response.text)
    except Exception as e:
        return str(e)

def B4(repo_url, commit_message):
    """Clone a Git repository and make a commit."""
    try:
        repo_path = "/data/repo"
        B12(repo_path)
        subprocess.run(["git", "clone", repo_url, repo_path], check=True)
        subprocess.run(["git", "-C", repo_path, "commit", "-m", commit_message], check=True)
    except Exception as e:
        return str(e)

def B5(db_path, query, output_filename):
    """Run a SQL query on a SQLite or DuckDB database."""
    try:
        B12(db_path)
        B12(output_filename)
        conn = sqlite3.connect(db_path) if db_path.endswith('.db') else duckdb.connect(db_path)
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchall()
        conn.close()
        with open(output_filename, 'w') as file:
            file.write(str(result))
        return result
    except Exception as e:
        return str(e)

def B6(url, output_filename):
    """Scrape data from a website."""
    try:
        B12(output_filename)
        result = requests.get(url).text
        with open(output_filename, 'w') as file:
            file.write(str(result))
    except Exception as e:
        return str(e)

def B7(image_path, output_path, resize=None):
    """Compress or resize an image."""
    try:
        B12(image_path)
        B12(output_path)
        img = Image.open(image_path)
        if resize:
            img = img.resize(resize)
        img.save(output_path)
    except Exception as e:
        return str(e)

def B8(audio_path):
    """Transcribe audio from an MP3 file."""
    try:
        import openai
        B12(audio_path)
        with open(audio_path, 'rb') as audio_file:
            return openai.Audio.transcribe("whisper-1", audio_file)
    except Exception as e:
        return str(e)

def B9(md_path, output_path):
    """Convert Markdown to HTML."""
    try:
        B12(md_path)
        B12(output_path)
        with open(md_path, 'r') as file:
            html = markdown.markdown(file.read())
        with open(output_path, 'w') as file:
            file.write(html)
    except Exception as e:
        return str(e)

@app.route('/filter_csv', methods=['POST'])
def B10():
    """Filter a CSV file and return JSON data."""
    try:
        data = request.json
        csv_path, filter_column, filter_value = data['csv_path'], data['filter_column'], data['filter_value']
        B12(csv_path)
        df = pd.read_csv(csv_path)
        filtered = df[df[filter_column] == filter_value]
        return jsonify(filtered.to_dict(orient='records'))
    except Exception as e:
        return jsonify({"error": str(e)})
