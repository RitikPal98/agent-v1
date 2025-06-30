import os
from flask import Flask, render_template, request, jsonify
import json
from main import LLMClient, SchemaDetectorAgent, ProfileMatchingAgent, Profile, CSVSource, JSONSource, SQLiteSource, recursive_match

app = Flask(__name__)

# Helper: List available data files
DATA_DIRS = ["test_data", "."]
CSV_EXT = ".csv"
JSON_EXT = ".json"
SQLITE_EXTS = [".db", ".sqlite", ".sqlite3"]

def list_data_files():
    files = {"csv": [], "json": [], "sqlite": []}
    for d in DATA_DIRS:
        for fname in os.listdir(d):
            fpath = os.path.join(d, fname)
            if os.path.isfile(fpath):
                if fname.endswith(CSV_EXT):
                    files["csv"].append(fpath)
                elif fname.endswith(JSON_EXT):
                    files["json"].append(fpath)
                elif any(fname.endswith(ext) for ext in SQLITE_EXTS):
                    files["sqlite"].append(fpath)
    return files

# Helper: Load file content

def load_file_content(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

# Helper: Get all table names from a SQLite DB
import sqlite3
def get_sqlite_tables(db_path):
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        tables = [row[0] for row in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        conn.close()
        return tables
    except Exception as e:
        return []

# Endpoint: List all available data sources
@app.route("/api/list_sources")
def api_list_sources():
    files = list_data_files()
    # For SQLite, also list tables
    sqlite_with_tables = []
    for db_path in files["sqlite"]:
        tables = get_sqlite_tables(db_path)
        sqlite_with_tables.append({"path": db_path, "tables": tables})
    return jsonify({"csv": files["csv"], "json": files["json"], "sqlite": sqlite_with_tables})

# Endpoint: Get schema for selected sources
@app.route("/api/schema", methods=["POST"])
def api_schema():
    data = request.json
    sources = []
    llm = LLMClient()
    schema_agent = SchemaDetectorAgent(llm)
    for src in data.get("sources", []):
        typ = src["type"]
        name = src["name"]
        if typ == "csv":
            csv_str = load_file_content(name)
            source = CSVSource(name, csv_str)
        elif typ == "json":
            json_str = load_file_content(name)
            source = JSONSource(name, json_str)
        elif typ == "sqlite":
            table = src.get("table")
            source = SQLiteSource(name, name, table)
        else:
            continue
        fields = schema_agent.detect(source)
        sources.append({"name": name, "type": typ, "fields": fields, "table": src.get("table")})
    return jsonify(sources)

# Endpoint: Run matching and return results
@app.route("/api/match", methods=["POST"])
def api_match():
    data = request.json
    base_profile = Profile(data["base_profile"])
    selected_sources = data["sources"]
    llm = LLMClient()
    sources = []
    for src in selected_sources:
        typ = src["type"]
        name = src["name"]
        if typ == "csv":
            csv_str = load_file_content(name)
            source = CSVSource(name, csv_str)
        elif typ == "json":
            json_str = load_file_content(name)
            source = JSONSource(name, json_str)
        elif typ == "sqlite":
            table = src.get("table")
            source = SQLiteSource(name, name, table)
        else:
            continue
        sources.append(source)
    results = recursive_match(base_profile, sources, llm, threshold=0.5)
    # Return ranked results, enriched profile, and raw JSON
    return jsonify({
        "ranked_results": results,
        "enriched_profile": base_profile.data,
        "raw_json": results
    })

@app.route("/")
def index():
    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True) 