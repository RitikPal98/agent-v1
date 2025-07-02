import os
from flask import Flask, render_template, request, jsonify
import json
from main import LLMClient, SchemaDetectorAgent, ProfileMatchingAgent, Profile, CSVSource, JSONSource, recursive_match, NLPreprocessorAgent, recursive_match_with_full_profiles

app = Flask(__name__)

# Helper: List available data files
DATA_DIRS = ["test_data", "."]
CSV_EXT = ".csv"
JSON_EXT = ".json"

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
                
    return files

# Helper: Load file content

def load_file_content(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


# Endpoint: List all available data sources
@app.route("/api/list_sources")
def api_list_sources():
    files = list_data_files()
    return jsonify({"csv": files["csv"], "json": files["json"]})

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

# Endpoint: Process natural language input to extract profile information
@app.route("/api/extract_profile", methods=["POST"])
def api_extract_profile():
    """Extract structured profile information from natural language input"""
    data = request.json
    natural_language_query = data.get("query", "")
    
    if not natural_language_query.strip():
        return jsonify({"error": "No query provided"}), 400
    
    try:
        llm = LLMClient()
        nl_processor = NLPreprocessorAgent(llm)
        
        # Extract profile information from natural language
        extracted_profile = nl_processor.extract_profile(natural_language_query)
        
        return jsonify({
            "success": True,
            "extracted_profile": extracted_profile,
            "original_query": natural_language_query
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "original_query": natural_language_query
        }), 500

# Enhanced endpoint: Run matching with natural language input support
@app.route("/api/match_nl", methods=["POST"])
def api_match_nl():
    """Run matching pipeline with either structured profile or natural language input"""
    data = request.json
    selected_sources = data["sources"]
    
    # Check if input is natural language or structured profile
    if "natural_language_query" in data and data["natural_language_query"].strip():
        # Process natural language input first
        try:
            llm = LLMClient()
            nl_processor = NLPreprocessorAgent(llm)
            extracted_data = nl_processor.extract_profile(data["natural_language_query"])
            base_profile = Profile(extracted_data)
            input_type = "natural_language"
            original_query = data["natural_language_query"]
        except Exception as e:
            return jsonify({
                "error": f"Failed to process natural language input: {str(e)}",
                "original_query": data.get("natural_language_query", "")
            }), 500
    else:
        # Use structured profile data
        base_profile = Profile(data["base_profile"])
        input_type = "structured"
        original_query = None
    
    # Continue with matching pipeline
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
        else:
            continue
        sources.append(source)
    
    # Use enhanced function that includes full profile data for better display
    results = recursive_match_with_full_profiles(base_profile, sources, llm, threshold=0.5)
    
    # Return enhanced results with input information
    return jsonify({
        "input_type": input_type,
        "original_query": original_query,
        "extracted_profile": base_profile.data,
        "ranked_results": results,
        "enriched_profile": base_profile.data,
        "raw_json": results
    })

@app.route("/")
def index():
    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True) 