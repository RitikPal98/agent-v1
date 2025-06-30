import os, json, sqlite3, csv, io
from typing import List, Dict, Any
from abc import ABC, abstractmethod
from dotenv import load_dotenv
from google import generativeai as genai
import sqlite3
import re
import ast  # add this at the top

load_dotenv()

class LLMClient:
    def __init__(self, model="gemini-2.5-turbo"):
        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError("GOOGLE_API_KEY not set")
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(model)

    def generate(self, prompt: str) -> str:
        return self.model.generate_content(prompt).text

class LLMClient:
    """
    Wraps the Google Gemini (GenAI) client. Loads the GEMINI_API_KEY from .env.
    """
    def __init__(self, model: str = "gemini-2.0-flash"):
        load_dotenv()
        self.api_key = os.getenv("GOOGLE_API_KEY")
        if not self.api_key:
            raise ValueError("GOOGLE_API_KEY not set in environment")
        
        # Configure the API
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel(model)

    def generate(self, prompt: str) -> str:
        """
        Generates text using the LLM with the given prompt.
        """
        try:
            response = self.model.generate_content(prompt)
            if response.text:
                # print(response.text)
                return response.text
            else:
                return ""
        except Exception as e:
            print(f"Error generating text: {e}")
            return ""

class SchemaDetectorAgent:
    def __init__(self, llm: LLMClient):
        self.llm = llm
        self.cache: Dict[str, List[str]] = {}

    def detect(self, source: "DataSource") -> List[str]:
        if source.name in self.cache:
            return self.cache[source.name]
        fields = source.infer_schema()
        self.cache[source.name] = fields
        return fields

    def align(self, base: List[str], target: List[str]) -> Dict[str, str]:
        prompt = (
            "You are an expert data engineer with deep experience in data integration, "
            "heterogeneous data systems, and intelligent schema alignment.\n"
            "Your task is to carefully analyze and map field names from a target schema "
            "to a base schema, even when naming conventions differ, fields are reordered, "
            "or contain domain-specific terminology.\n\n"
            "Given:\n"
            f"🔹 Base Schema Fields (reference): {base}\n"
            f"🔹 Target Schema Fields (to align): {target}\n\n"
            "Instructions:\n"
            "- Map each field in the TARGET schema to its most likely equivalent in the BASE schema.\n"
            "- The mapping should be based on semantic meaning, common synonyms, and domain context (e.g., 'dob' ↔ 'birth_date').\n"
            "- If no good mapping exists for a target field, omit it from the mapping.\n"
            "- Do NOT include base fields that are not matched by any target field.\n\n"
            "Return JSON mapping from target → base."
        )
        resp = self.llm.generate(prompt)

        try:
            # Try extracting JSON inside triple backticks (```json ... ```)
            match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', resp, re.DOTALL)
            json_str = match.group(1) if match else resp.strip()

            print("🔍 Raw extracted JSON:\n", json_str)

            # First, try standard JSON parsing
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            print(f"⚠️ json.loads failed: {e}")
            try:
                # Try using ast.literal_eval as fallback for non-standard JSON (e.g. single quotes)
                return ast.literal_eval(json_str)
            except Exception as e2:
                print(f"❌ Failed to parse mapping even with ast.literal_eval: {e2}")
                return {}


class Profile:
    def __init__(self, data: Dict[str, Any]):
        self.data = data

class ProfileMatchingAgent:
    def __init__(self, llm: LLMClient):
        self.llm = llm

    def compare(self, base: Profile, cand: Profile) -> Dict[str, Any]:
        prompt = (
            "You are an expert identity resolution engineer with deep experience in KYC, fraud detection, "
            "and profile matching across fragmented or incomplete data sources.\n\n"
            "Your task is to analyze and compare two identity profiles to assess the likelihood that they refer "
            "to the same real-world individual. These profiles may differ in formatting, field presence, or data quality.\n\n"
            "Instructions:\n"
            "- Carefully evaluate each shared field (e.g., name, email, DOB, phone, address, customer_id).\n"
            "- Weigh strong matches like exact name and DOB more heavily.\n"
            "- Missing fields should reduce confidence only slightly unless they are critical.\n"
            "- Consider fuzzy matches (e.g., email domain differences or name variants).\n"
            "- Return your evaluation as a JSON object.\n\n"
            "Input:\n"
            f"🔹 Base Profile: {json.dumps(base.data)}\n"
            f"🔹 Candidate Profile: {json.dumps(cand.data)}\n\n"
            "Output Format:\n"
            "{\n"
            '  "score": float (between 0 and 1),\n'
            '  "reason": "short explanation of your logic"\n'
            "}\n\n"
            "Respond ONLY with the JSON object. No extra explanation or markdown."
        )
        # print(json.dumps(base.data))
        # print(json.dumps(cand.data))
        resp = self.llm.generate(prompt)
        # print("\n📨 RAW LLM RESPONSE:\n", resp) 
        try:
            match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', resp, re.DOTALL)
            json_str = match.group(1) if match else resp.strip()
            return json.loads(json_str)
        except:
            return {"score": 0.0, "reason": resp}

class DataSource(ABC):
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def infer_schema(self) -> List[str]: ...
    @abstractmethod
    def get_profiles(self) -> List[Profile]: ...

class CSVSource(DataSource):
    def __init__(self, name: str, csv_str: str):
        super().__init__(name)
        self.csv_str = csv_str

    def infer_schema(self):
        reader = csv.DictReader(io.StringIO(self.csv_str))
        return reader.fieldnames or []

    def get_profiles(self):
        reader = csv.DictReader(io.StringIO(self.csv_str))
        return [Profile(dict(row)) for row in reader]

class SQLiteSource(DataSource):
    def __init__(self, name: str, db_path: str, table: str):
        super().__init__(name)
        self.db_path = db_path
        self.table = table

    def infer_schema(self):
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute(f"PRAGMA table_info({self.table})").fetchall()
        conn.close()
        return [r[1] for r in rows]

    def get_profiles(self):
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute(f"SELECT * FROM {self.table}").fetchall()
        cols = [d[0] for d in conn.execute(f"SELECT * FROM {self.table}").description]
        return [Profile({cols[i]: row[i] for i in range(len(cols))}) for row in rows]

class JSONSource(DataSource):
    def __init__(self, name: str, json_str: str):
        super().__init__(name)
        self.json_str = json_str

    def infer_schema(self):
        data = json.loads(self.json_str)
        if not data:
            return []
        if isinstance(data, list):
            return list({k for obj in data for k in obj.keys()})
        return list(data.keys())

    def get_profiles(self):
        data = json.loads(self.json_str)
        if isinstance(data, list):
            return [Profile(obj) for obj in data]
        return [Profile(data)]

def recursive_match(base: Profile, sources: List[DataSource], llm: LLMClient, threshold=0.5) -> List[Dict[str, Any]]:
    schema_agent = SchemaDetectorAgent(llm)
    matcher = ProfileMatchingAgent(llm)
    results = []

    for src in sources:
        base_fields = list(base.data.keys())
        target_fields = schema_agent.detect(src)
        mapping = schema_agent.align(base_fields, target_fields)
        # print(mapping)

        for p in src.get_profiles():
            normalized = {}
            for k, v in p.data.items():
                base_field = mapping.get(k)
                if base_field:
                    normalized[base_field] = v.strip() if isinstance(v, str) else v
                    print(normalized)

            candidate = Profile(normalized)
            res = matcher.compare(base, candidate)
            score = res.get("score", 0)
            print(f"📦 Source: {src.name}")
            # print(f"   Raw profile: {p.data}")
            # print(f"   Field mapping: {mapping}")
            # print(f"   Normalized: {normalized}")
            if score >= threshold:
                print(f"\n✅ MATCH FOUND from {src.name} with score {score:.2f}")
                for k, v in normalized.items():
                    if k not in base.data or base.data[k] in (None, "", []):
                        print(f"🔧 Enriching '{k}' → '{v}'")
                        base.data[k] = v
                res.update({"source": src.name, "candidate": normalized})
                results.append(res)
    return sorted(results, key=lambda x: x["score"], reverse=True)



if __name__ == "__main__":

    # Initialize the LLM client
    llm = LLMClient()

    # Base profile with partial information
    base_profile = Profile({
        "name": "John Smith",
        "dob": "1988-01-01",
        "email": "",  # Missing info to be enriched
        "customer_id": "CUST1234"
    })

    # Simulated heterogeneous data sources

    # CSV Source (simulates no header and missing fields)
    csv_data = """name,dob,email,address
    John Smith,1988-01-01,john@example.com,123 Main St
    Jane Doe,1990-05-10,jane@example.com,456 Oak Ave"""
    csv_source = CSVSource("CustomerCSV", csv_data)

    # JSON Source (missing dob but has customer_id)
    json_data = """[
    {"name": "John Smith", "customer_id": "CUST1234", "phone": "9999999999"},
    {"name": "Alice", "customer_id": "CUST5555"}
    ]"""
    json_source = JSONSource("CRM_JSON", json_data)

    # SQLite Source (requires a .db file, here we create a sample in-memory one)
    
    conn = sqlite3.connect("test_customers.db")
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS customers")
    cur.execute("""
    CREATE TABLE customers (
        full_name TEXT,
        birthdate TEXT,
        email TEXT,
        customer_id TEXT
    )
    """)
    cur.execute("INSERT INTO customers VALUES (?, ?, ?, ?)",
                ("John Smith", "1988-01-01", "john.alt@example.com", "CUST1234"))
    conn.commit()
    conn.close()

    sqlite_source = SQLiteSource("SQL_Customers", "test_customers.db", "customers")

    # List of data sources
    sources = [csv_source, json_source, sqlite_source]

    # Perform recursive matching and enrichment
    results = recursive_match(base_profile, sources, llm, threshold=0.1)
    print(results)

    # Output results
    print("\n🎯 Matched Candidates with Confidence Scores:\n")
    for i, r in enumerate(results, 1):
        print(f"{i}. Source: {r['source']}")
        print(f"   Score: {r['score']:.2f}")
        print(f"   Reason: {r['reason']}")
        print(f"   Candidate Data: {r['candidate']}")
        print("-" * 60)

    print("\n🧠 Final Enriched Base Profile:")
    print(base_profile.data)

