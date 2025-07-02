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


class NLPreprocessorAgent:
    """
    Agent 0: Natural Language Preprocessor
    Extracts structured profile information from natural language queries.
    """
    
    def __init__(self, llm: LLMClient):
        self.llm = llm
        # Define supported field types for dynamic extraction
        self.supported_fields = {
            "name": ["name", "full_name", "first_name", "last_name", "given_name", "surname"],
            "dob": ["dob", "date_of_birth", "birth_date", "birthdate", "born"],
            "id": ["id", "customer_id", "user_id", "account_id", "identification", "identifier"],
            "phone": ["phone", "phone_number", "mobile", "cell", "telephone", "contact"],
            "email": ["email", "email_address", "mail", "e_mail"],
            "address": ["address", "location", "residence", "home", "city", "state", "country"],
            "bank_id": ["bank_id", "bank_account", "account_number", "banking_id"],
            "passport": ["passport", "passport_number", "passport_id"],
            "ssn": ["ssn", "social_security", "social_security_number"],
            "nationality": ["nationality", "citizenship", "country_of_birth"],
            "occupation": ["occupation", "job", "profession", "work", "employment"],
            "company": ["company", "employer", "organization", "firm", "workplace"]
        }

    def extract_profile(self, natural_language_query: str) -> Dict[str, Any]:
        """
        Extract structured profile information from natural language input.
        
        Args:
            natural_language_query: Natural language text containing profile information
            
        Returns:
            Dictionary with extracted fields in clean JSON format
        """
        
        # Create field descriptions for the prompt
        field_descriptions = []
        for field_type, variations in self.supported_fields.items():
            field_descriptions.append(f"- {field_type}: {', '.join(variations)}")
        
        prompt = f"""You are an expert information extraction agent specializing in profile data extraction from natural language.

Your task is to analyze the following natural language query and extract ALL possible profile information into a structured JSON format.

SUPPORTED FIELD TYPES:
{chr(10).join(field_descriptions)}

INPUT TEXT: "{natural_language_query}"

EXTRACTION RULES:
1. Extract ONLY the information that is explicitly mentioned in the text
2. Use standardized field names (name, dob, id, phone, email, address, bank_id, passport, ssn, nationality, occupation, company)
3. DO NOT invent or assume any information not present in the text
4. Handle dates in a consistent format when possible
5. Clean and normalize extracted values (remove extra spaces, standardize formats)
6. If multiple values exist for the same field type, choose the most complete one
7. Return ONLY the JSON object, no explanations or markdown

EXAMPLE OUTPUT FORMAT:
{{
  "name": "John Doe",
  "dob": "1990-01-15",
  "phone": "9999999999",
  "email": "john@example.com",
  "address": "New York"
}}

Extract the profile information and respond with ONLY the JSON object:"""

        try:
            response = self.llm.generate(prompt)
            
            # Clean and parse the JSON response
            return self._parse_json_response(response)
            
        except Exception as e:
            print(f"❌ Error in NL extraction: {e}")
            return {}

    def _parse_json_response(self, response: str) -> Dict[str, Any]:
        """Parse and clean JSON response from LLM"""
        try:
            # Try extracting JSON inside triple backticks first
            match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', response, re.DOTALL)
            json_str = match.group(1) if match else response.strip()
            
            # Remove any leading/trailing whitespace and non-JSON content
            json_str = json_str.strip()
            
            # Find the first { and last } to extract clean JSON
            start_idx = json_str.find('{')
            end_idx = json_str.rfind('}')
            
            if start_idx != -1 and end_idx != -1:
                json_str = json_str[start_idx:end_idx+1]
            
            # Try standard JSON parsing
            parsed_data = json.loads(json_str)
            
            # Clean and validate the extracted data
            return self._clean_extracted_data(parsed_data)
            
        except json.JSONDecodeError:
            try:
                # Fallback to ast.literal_eval for single-quoted JSON
                parsed_data = ast.literal_eval(json_str)
                return self._clean_extracted_data(parsed_data)
            except Exception as e:
                print(f"⚠️ Failed to parse NL extraction response: {e}")
                print(f"Raw response: {response}")
                return {}

    def _clean_extracted_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize extracted profile data"""
        cleaned = {}
        
        for key, value in data.items():
            if value and str(value).strip():  # Only include non-empty values
                key = key.lower().strip()
                
                # Normalize string values
                if isinstance(value, str):
                    value = value.strip()
                    # Remove quotes if present
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                
                # Map common variations to standard field names
                standardized_key = self._standardize_field_name(key)
                if standardized_key:
                    cleaned[standardized_key] = value
        
        return cleaned

    def _standardize_field_name(self, field_name: str) -> str:
        """Map field variations to standard field names"""
        field_name = field_name.lower().strip().replace(' ', '_')
        
        for standard_name, variations in self.supported_fields.items():
            if field_name in [v.lower().replace(' ', '_') for v in variations]:
                return standard_name
        
        # If no exact match, return the original field name
        return field_name

    def create_profile_from_nl(self, natural_language_query: str) -> Profile:
        """
        Convenience method to create a Profile object directly from natural language.
        
        Args:
            natural_language_query: Natural language text containing profile information
            
        Returns:
            Profile object with extracted data
        """
        extracted_data = self.extract_profile(natural_language_query)
        return Profile(extracted_data)


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


def recursive_match_with_full_profiles(base: Profile, sources: List[DataSource], llm: LLMClient, threshold=0.5) -> List[Dict[str, Any]]:
    """
    Enhanced version of recursive_match that includes full profile data in results.
    This function calls the original recursive_match and then enhances results with complete profile information.
    """
    schema_agent = SchemaDetectorAgent(llm)
    matcher = ProfileMatchingAgent(llm)
    results = []

    for src in sources:
        base_fields = list(base.data.keys())
        target_fields = schema_agent.detect(src)
        mapping = schema_agent.align(base_fields, target_fields)

        for p in src.get_profiles():
            normalized = {}
            for k, v in p.data.items():
                base_field = mapping.get(k)
                if base_field:
                    normalized[base_field] = v.strip() if isinstance(v, str) else v

            candidate = Profile(normalized)
            res = matcher.compare(base, candidate)
            score = res.get("score", 0)
            print(f"📦 Source: {src.name}")
            
            if score >= threshold:
                print(f"\n✅ MATCH FOUND from {src.name} with score {score:.2f}")
                for k, v in normalized.items():
                    if k not in base.data or base.data[k] in (None, "", []):
                        print(f"🔧 Enriching '{k}' → '{v}'")
                        base.data[k] = v
                
                # Include both normalized data (for enrichment) and full original profile data (for display)
                res.update({
                    "source": src.name, 
                    "candidate": normalized,  # Normalized/mapped fields for enrichment
                    "full_profile": p.data,   # Complete original profile data for display
                    "field_mapping": mapping  # Field mapping for reference
                })
                results.append(res)
    
    return sorted(results, key=lambda x: x["score"], reverse=True)

