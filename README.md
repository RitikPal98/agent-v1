# Advanced Name Screening System

## 🆕 Natural Language Input Support

The system now supports **natural language queries** for profile extraction! Users can input ChatGPT-style text, and the system will automatically extract structured profile information.

### Example Usage:

**Input:**

> "Find details of a person named Rahul Mehra, born on 10 Feb 1990, ID is RHM123, phone number 9990008888, and he lives in Delhi."

**Extracted Output:**

```json
{
  "name": "Rahul Mehra",
  "dob": "10 Feb 1990",
  "id": "RHM123",
  "phone": "9990008888",
  "address": "Delhi"
}
```

### 🎯 Key Features:

1. **Agent 0 (NL Preprocessor)**: Extracts structured fields from natural language
2. **Dynamic Field Detection**: Supports name, dob, id, phone, email, address, bank_id, passport, etc.
3. **Seamless Integration**: Works with existing schema detection and profile matching pipeline
4. **Dual Input Mode**: Web interface supports both natural language and structured input

### 🛠 API Endpoints:

- `/api/extract_profile` - Extract profile from natural language
- `/api/match_nl` - Complete pipeline with natural language input
- `/api/match` - Original structured input endpoint (still available)

### 📦 Module Import:

```python
from agents import NLPreprocessor, LLMClient

llm = LLMClient()
nl_agent = NLPreprocessor(llm)

# Extract profile from natural language
profile_data = nl_agent.extract_profile("John Doe, born 1990, phone 555-1234")

# Create Profile object directly
profile = nl_agent.create_profile_from_nl("John Doe, born 1990, phone 555-1234")
```

### 🧪 Testing:

Run the test script to see the NL preprocessing in action:

```bash
python test_nl_agent.py
```

### 🚀 Getting Started:

1. Set your `GOOGLE_API_KEY` in `.env` file
2. Install dependencies: `pip install -r requirements.txt`
3. Run the Flask app: `python app.py`
4. Visit http://localhost:5000 and try the "Natural Language" tab!
