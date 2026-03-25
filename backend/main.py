import os
import re
import json
import logging
import httpx
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from db import build_graph, get_broken_flows, run_sql, SCHEMA_DESCRIPTION

app = FastAPI(title="SAP O2C Backend")

# Add CORS middleware - allow all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

load_dotenv()
# Configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_ENDPOINT = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"

# Request/Response models
class ChatMessage(BaseModel):
    role: str  # "user" or "assistant"
    content: str


class ChatRequest(BaseModel):
    message: str
    history: Optional[List[ChatMessage]] = None


class ChatResponse(BaseModel):
    answer: str
    sql: Optional[str] = None
    explanation: Optional[str] = None
    data: Optional[List[Dict[str, Any]]] = None
    referenced_ids: Dict[str, List[str]] = {}
    row_count: Optional[int] = None


# System prompt for Gemini URL Generation
SYSTEM_PROMPT = f"""You are an expert SQL analyst for the SAP Order-to-Cash (O2C) dataset.

## Schema Reference
{SCHEMA_DESCRIPTION}

## Instructions
1. Answer ONLY questions about the SAP Order-to-Cash dataset provided above.
2. Generate DuckDB SQL SELECT statements to answer the user's question. If the user asks an open-ended question (like "trace a flow" without specifying an ID), select a representative sample of rows (LIMIT 5). If they provide a specific ID, filter for that exact ID.
3. CRITICAL: Whenever possible, INCLUDE the primary keys/IDs of the relevant entities in your SELECT statement (like businessPartner, salesOrder, deliveryDocument, billingDocument, accountingDocument). We need these IDs in the results to highlight them in the UI graph!

### REQUIRED FOR AGGREGATIONS - EXACT COLUMN NAMING ###
If you use GROUP BY for aggregations (like COUNT or SUM):
- MANDATORY: Use `COUNT(DISTINCT [column])` format - never just `COUNT()`
- MANDATORY: Include `STRING_AGG(DISTINCT [ID_column], ',') AS [entityName]s`
- Use CONSISTENT column naming in aliases: if COUNT result is named, use descriptive names like `number_of_items` or `total_amount`
- Do NOT include redundant/duplicate columns (if you SELECT a column already, don't SELECT it again)
- Do NOT add extra columns that weren't explicitly requested

EXAMPLE FOR PRODUCT-BILLING QUERY (FOLLOW THIS PATTERN):
SELECT 
  pd.product,
  pd.productDescription,
  COUNT(DISTINCT bdi.billingDocument) AS number_of_billing_documents,
  STRING_AGG(DISTINCT bdi.billingDocument, ',') AS billing_documents
FROM product_descriptions AS pd
JOIN billing_document_items AS bdi ON pd.product = bdi.material
WHERE pd.language = 'EN'
GROUP BY pd.product, pd.productDescription
ORDER BY number_of_billing_documents DESC
LIMIT 20
4. Use TRY_CAST() for numeric string conversions.
5. Limit results to 20 rows maximum.
6. Only SELECT statements are allowed. No INSERT, UPDATE, DELETE, or DDL.
7. Handle NULL values appropriately.
8. OPTIMIZE JOINS: Always use the shortest available JOIN path documented in the Schema Reference. For example, to link Billing Documents to Products, use the direct path via `billing_document_items.material`, avoiding intermediate tables (like Sales Orders/Deliveries) to prevent dropping rows on incomplete sample data.
9. TRACE FLOW QUERIES (if user asks to trace/follow a flow without specifying an ID): IMPORTANT - Not all billing documents have complete flows to journal entries. Use a flexible query that:
   - Automatically finds a representative complete flow example (WHERE bd.accountingDocument IS NOT NULL AND EXISTS (SELECT 1 FROM journal_entry_items_accounts_receivable WHERE accountingDocument = bd.accountingDocument))
   - AND use IS NOT NULL checks on joining columns when they are known to have gaps.

## CRITICAL TABLE NAME MAPPING (memorize these):
- NEVER use "delivery_items" → correct name is "outbound_delivery_items"
- NEVER use "deliveries" or "delivery_headers" → correct name is "outbound_delivery_headers"  
- NEVER use "billing_documents" → correct name is "billing_document_headers"
- NEVER use "journal_entries" → correct name is "journal_entry_items_accounts_receivable"
- NEVER use "payments" → correct name is "payments_accounts_receivable"
These are the ONLY valid table names. Using any other name will cause SQL errors.

## Response Format
You MUST respond with a valid JSON object ONLY. Do not use markdown code blocks:
{{
  "sql": "SELECT ... FROM ... LIMIT 20",
  "explanation": "Brief explanation of what the query does",
  "referenced_ids": {{}}
}}

CRITICAL: For consistency across multiple executions, follow these exact rules:
- Use snake_case for all multi-word column names (number_of_items, not numberOfItems)
- Always use COUNT(DISTINCT column) for counting distinct values
- Never include the same column twice in SELECT (no duplicates)
- For aggregations with GROUP BY, always include the STRING_AGG column for IDs
- Example for product query: SELECT product, product_description, COUNT(DISTINCT billing_document) AS number_of_billing_documents, STRING_AGG(DISTINCT billing_document, ',') AS billing_documents FROM ... GROUP BY product, product_description
"""

INTENT_SYSTEM_PROMPT = """You are an expert Intent Classifier and Query Refiner for an SAP Order-to-Cash (O2C) dataset.
The dataset covers: Business Partners, Sales Orders, Deliveries, Billing Documents, Journal Entries, and Products.

Your tasks:
1. RELEVANCE CHECK: Determine if the user's message is related to querying or analyzing the SAP O2C database.
   - General knowledge questions, jokes, creative writing, or completely irrelevant topics MUST be marked as off_topic: true.
2. QUERY REFINEMENT: If the query is relevant but vague, refine it into a highly explicit data-retrieval instruction for a SQL analyst. Make sure vague terms are clarified based on the O2C domain. Include necessary context.

You MUST respond with a valid JSON object ONLY:
{
  "off_topic": false,
  "refined_query": "The clarified, highly explicit data retrieval instruction (if relevant, otherwise null)",
  "reasoning": "Brief explanation for relevance or how it was refined"
}
"""


def is_off_topic(message: str) -> bool:
    """Check if message is off-topic (general knowledge, jokes, weather, etc.)."""
    message_lower = message.lower()
    
    off_topic_keywords = [
        "joke", "funny", "meme", "weather", "temperature", "recipe", "music",
        "movie", "book", "sport", "game", "python tutorial", "how to code",
        "machine learning tutorial", "creative writing", "story", "poem",
        "history of", "who is", "what is", "define", "meaning of",
        "tutorial", "guide", "how to", "tips and tricks"
    ]
    
    # Simple keyword-based off-topic detection
    for keyword in off_topic_keywords:
        if keyword in message_lower:
            return True
    
    return False


def extract_json_from_text(text: str) -> Optional[Dict[str, Any]]:
    """Extract JSON from text, handling markdown code fences."""
    # Remove markdown code fences
    text = re.sub(r"```(json)?\n?", "", text)
    text = re.sub(r"```\n?", "", text)
    
    # Try to find JSON block (first {...})
    json_match = re.search(r"\{.*\}", text, re.DOTALL)
    
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            return None
    
    return None

def extract_ids_from_data(data: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """Automatically extract entity IDs from SQL result data to ensure highlighting works."""
    refs = {
        "customers": set(),
        "sales_orders": set(),
        "deliveries": set(),
        "billing_documents": set(),
        "journal_entries": set(),
        "payments": set()
    }
    
    if not data:
        return {k: list(v) for k, v in refs.items()}
        
    for row in data:
        for k, v in row.items():
            if not v: continue
            k_clean = k.lower().replace("_", "")
            
            # Handle list array or comma-separated strings inside cells (for mapped aggregations)
            if isinstance(v, list):
                val_strs = [str(x).strip() for x in v if x]
            elif isinstance(v, str) and ',' in v:
                val_strs = [x.strip() for x in v.split(",") if x.strip()]
            else:
                val_strs = [str(v).strip()]
                
            for val_str in val_strs:
                if not val_str:
                    continue
                if "businesspartner" in k_clean or "soldtoparty" in k_clean or "customer" in k_clean:
                    if val_str.isdigit():
                        refs["customers"].add(val_str)
                elif "salesorder" in k_clean or "referencesddocument" in k_clean:
                    if val_str.startswith("7"):
                        refs["sales_orders"].add(val_str)
                    elif val_str.startswith("8"):
                        refs["deliveries"].add(val_str)
                elif "delivery" in k_clean:
                    if val_str.startswith("8"):
                        refs["deliveries"].add(val_str)
                elif "billingdocument" in k_clean or "referencedocument" in k_clean:
                    if val_str.startswith("9"):
                        refs["billing_documents"].add(val_str)
                elif "accountingdocument" in k_clean:
                    refs["journal_entries"].add(val_str)
                    refs["payments"].add(val_str)
                
    return {k: list(v) for k, v in refs.items()}


def normalize_column_names(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert all column names to snake_case for consistency."""
    if not data:
        return data
    
    def to_snake_case(name: str) -> str:
        """Convert camelCase to snake_case."""
        result = []
        for i, char in enumerate(name):
            if char.isupper() and i > 0:
                result.append('_')
                result.append(char.lower())
            else:
                result.append(char.lower())
        return ''.join(result)
    
    normalized = []
    for row in data:
        new_row = {to_snake_case(k): v for k, v in row.items()}
        normalized.append(new_row)
    
    return normalized


async def call_gemini_for_intent(user_message: str) -> Dict[str, Any]:
    """Call Gemini API to check intent and refine the query."""
    if not GEMINI_API_KEY:
        raise HTTPException(status_code=500, detail="GEMINI_API_KEY environment variable not set")
    
    payload = {
        "system_instruction": {
            "parts": [{"text": INTENT_SYSTEM_PROMPT}]
        },
        "contents": [
            {
                "role": "user",
                "parts": [{"text": f"User's request: {user_message}"}]
            }
        ],
        "generationConfig": {
            "responseMimeType": "application/json",
            "temperature": 0.0
        }
    }
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{GEMINI_ENDPOINT}?key={GEMINI_API_KEY}",
                json=payload,
                timeout=30.0
            )
            response.raise_for_status()
        except httpx.HTTPError as e:
            raise HTTPException(status_code=500, detail=f"Gemini API error (Intent): {str(e)}")
            
    data = response.json()
    try:
        text = data["candidates"][0]["content"]["parts"][0]["text"]
        result = extract_json_from_text(text)
        return result if result else {"off_topic": False, "refined_query": user_message}
    except Exception:
        return {"off_topic": False, "refined_query": user_message}


async def call_gemini_for_sql(user_message: str, history: Optional[List[ChatMessage]] = None) -> Optional[Dict[str, Any]]:
    """Call Gemini API to generate SQL."""
    if not GEMINI_API_KEY:
        raise HTTPException(status_code=500, detail="GEMINI_API_KEY environment variable not set")
    
    # Validate constraints on role turns for Gemini 
    # Must end with user, and cannot have consecutive same roles. We will just send history properly.
    contents = []
    
    if history:
        for msg in history:
            role = "model" if msg.role == "assistant" else "user"
            content_text = msg.content if msg.content else " "
            
            # Avoid consecutive same-role messages
            if contents and contents[-1]["role"] == role:
                contents[-1]["parts"][0]["text"] += f"\n\n{content_text}"
            else:
                contents.append({
                    "role": role,
                    "parts": [{"text": content_text}]
                })
    
    # Add current user message
    if contents and contents[-1]["role"] == "user":
        contents[-1]["parts"][0]["text"] += f"\n\n{user_message}"
    else:
        contents.append({
            "role": "user",
            "parts": [{"text": user_message}]
        })
    
    payload = {
        "system_instruction": {
            "parts": [{"text": SYSTEM_PROMPT}]
        },
        "contents": contents,
        "generationConfig": {
            "responseMimeType": "application/json"
        }
    }
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{GEMINI_ENDPOINT}?key={GEMINI_API_KEY}",
                json=payload,
                timeout=30.0
            )
            response.raise_for_status()
        except httpx.HTTPError as e:
            raise HTTPException(status_code=500, detail=f"Gemini API error: {str(e)}")
    
    data = response.json()
    
    # Extract text from response
    try:
        text = data["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError, TypeError) as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse Gemini response: {str(e)}")
    
    # Extract JSON from text
    result = extract_json_from_text(text)
    
    if not result:
        raise HTTPException(status_code=500, detail="Failed to extract JSON from Gemini response")
    
    return result


async def call_gemini_for_answer(sql: str, data: List[Dict[str, Any]], user_message: str) -> Dict[str, Any]:
    """Call Gemini API to generate natural language answer from SQL results."""
    if not GEMINI_API_KEY:
        raise HTTPException(status_code=500, detail="GEMINI_API_KEY environment variable not set")
    
    # Prepare the prompt with results
    data_summary = json.dumps(data, indent=2, default=str)
    
    answer_prompt = f"""Based on the SQL query results below, provide a concise natural language answer to the user's original question.

Original Question: {user_message}

SQL Query Results:
{data_summary}

Respond with a JSON object containing:
{{
  "answer": "Natural language answer here",
  "referenced_ids": {{    "customers": [...],    "sales_orders": [...],
    "deliveries": [...],
    "billing_documents": [...],
    "journal_entries": [...],
    "payments": [...]
  }}
}}

Only return JSON, no markdown or preamble."""
    
    payload = {
        "system_instruction": {
            "parts": [{"text": "You are a helpful analysis assistant for SAP Order-to-Cash data."}]
        },
        "contents": [
            {
                "role": "user",
                "parts": [{"text": answer_prompt}]
            }
        ],
        "generationConfig": {
            "responseMimeType": "application/json"
        }
    }
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{GEMINI_ENDPOINT}?key={GEMINI_API_KEY}",
                json=payload,
                timeout=30.0
            )
            response.raise_for_status()
        except httpx.HTTPError as e:
            raise HTTPException(status_code=500, detail=f"Gemini API error: {str(e)}")
    
    data_response = response.json()
    
    # Extract text from response
    try:
        text = data_response["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError, TypeError) as e:
        raise HTTPException(status_code=500, detail=f"Failed to parse Gemini response: {str(e)}")
    
    # Extract JSON from text
    result = extract_json_from_text(text)
    
    if not result:
        # Return a basic response if we can't parse JSON
        result = {
            "answer": text,
            "referenced_ids": {}
        }
    
    return result


# ========== ENDPOINTS ==========

@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/graph")
async def graph():
    """Retrieve the full O2C graph with nodes and edges."""
    try:
        logger.info("Building graph...")
        graph_data = build_graph()
        logger.info(f"Graph built: {len(graph_data.get('nodes', []))} nodes, {len(graph_data.get('edges', []))} edges")
        return graph_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error building graph: {str(e)}")


@app.get("/broken-flows")
async def broken_flows():
    """Identify broken flows in the O2C process."""
    try:
        logger.info("Fetching broken flows...")
        flows = get_broken_flows()
        return flows
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving broken flows: {str(e)}")


@app.post("/chat")
async def chat(request: ChatRequest) -> ChatResponse:
    """
    Chat endpoint for natural language queries on the O2C dataset.
    
    Multi-step process: & Refine prompt via Gemini
    2. Generate SQL via Gemini using Refined prompt
    3. Execute SQL
    4. Generate natural language answer via Gemini
    """
    user_message = request.message.strip()
    logger.info(f"Chat request: {user_message}")
    
    if not user_message:
        raise HTTPException(status_code=400, detail="Message cannot be empty")
    
    # Step 1: Call Gemini to classify intent and refine the prompt
    try:
        intent_response = await call_gemini_for_intent(user_message)
    except Exception as e:
        logger.error(f"Error in intent classification: {e}")
        intent_response = {"off_topic": False, "refined_query": user_message}

    if intent_response.get("off_topic"):
        logger.info("Message classified as off-topic.")
        return ChatResponse(
            answer="This system is designed to answer questions related to the provided dataset only.",
            sql=None,
            data=None,
            referenced_ids={}
        )
        
    refined_query = intent_response.get("refined_query", user_message)
    logger.info(f"Refined query: {refined_query}")
    
    # Step 2: Call Gemini to generate SQL using refined query
    try:
        gemini_sql_response = await call_gemini_for_sql(refined_query, request.history)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calling Gemini: {str(e)}")
    
    sql = gemini_sql_response.get("sql")
    explanation = gemini_sql_response.get("explanation")
    referenced_ids = gemini_sql_response.get("referenced_ids", {})
    logger.info(f"Generated SQL: {sql if sql else 'None'}")
    
    # Step 3: Validate and execute SQL
    if not sql:
        return ChatResponse(
            answer="Could not generate a valid SQL query. Please rephrase your question.",
            sql=None,
            data=None,
            referenced_ids={}
        )
    
    # Only allow SELECT statements
    sql_upper = sql.strip().upper()
    if not sql_upper.startswith("SELECT"):
        raise HTTPException(status_code=400, detail="Only SELECT statements are allowed")
    
    # Step 3: Execute SQL - returns (rows, error_message)
    data, sql_error = run_sql(sql)
    logger.info(f"SQL result: {len(data) if data else 0} rows, error={sql_error is not None}")
    
    # Step 4: If SQL failed, call Gemini to fix it and retry ONCE
    if sql_error is not None:
        fix_prompt = f"""The SQL you generated failed with this error:
{sql_error}

The original question was: {user_message}

Fix the SQL. Remember these critical schema facts:
- product_descriptions.product is the primary key (NOT material). Join with billing_document_items ON pd.product = bdi.material. Join with sales_order_items ON pd.product = soi.material
- outbound_delivery_headers table has NO salesOrder column
- outbound_delivery_items.referenceSdDocument links to sales_order_headers.salesOrder
- billing_document_items.referenceSdDocument links to outbound_delivery_headers.deliveryDocument or sales_order_headers.salesOrder
- journal_entries.accountingDocumentItem is the line item field
- Payments link to journal_entries via the clearingAccountingDocument field
- Table "delivery_items" should be "outbound_delivery_items"
- Table "deliveries" or "delivery_headers" should be "outbound_delivery_headers"
- Table "billing_documents" should be "billing_document_headers"
- Table "journal_entries" should be "journal_entry_items_accounts_receivable"
- For UNION ALL queries: Remove the LIMIT 20 from individual SELECT statements before the UNION — DuckDB requires LIMIT only at the very end of the final UNION query.

Return corrected JSON with only the sql field:
{{"sql": "SELECT ... LIMIT 20"}}

Only return JSON, no markdown."""
        
        try:
            fix_payload = {
                "system_instruction": {
                    "parts": [{"text": "You are an expert DuckDB SQL corrector. Fix the SQL and return JSON with corrected query."}]
                },
                "contents": [{"role": "user", "parts": [{"text": fix_prompt}]}],
                "generationConfig": {
                    "responseMimeType": "application/json"
                }
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{GEMINI_ENDPOINT}?key={GEMINI_API_KEY}",
                    json=fix_payload,
                    timeout=30.0
                )
                response.raise_for_status()
            
            fix_response = response.json()
            fix_text = fix_response["candidates"][0]["content"]["parts"][0]["text"]
            fix_json = extract_json_from_text(fix_text)
            
            if fix_json and fix_json.get("sql"):
                corrected_sql = fix_json["sql"]
                # Retry with corrected SQL
                data, sql_error = run_sql(corrected_sql)
                if sql_error is None:
                    sql = corrected_sql
        except Exception:
            pass  # If fix attempt fails, proceed with original error
    
    # If still have an error, return error response
    if sql_error is not None and not data:
        return ChatResponse(
            answer="I could not query the data for that question. Please try rephrasing.",
            sql=sql,
            explanation=explanation,
            data=None,
            referenced_ids=referenced_ids,
            row_count=0
        )
    
    # Limit to 20 rows
    if len(data) > 20:
        data = data[:20]
    
    # Normalize all column names to snake_case for consistency across executions
    data = normalize_column_names(data)
        
    # Automatically extract highlighted IDs from the SQL output
    auto_ids = extract_ids_from_data(data)
    
    # Step 5: Call Gemini to generate natural language answer from results
    try:
        if data:
            gemini_answer_response = await call_gemini_for_answer(sql, data, user_message)
            answer = gemini_answer_response.get("answer", "Could not generate answer")
            
            # CRITICAL FIX: Only use `auto_ids` cleanly extracted from the actual DuckDB query results. 
            # Do NOT use Gemini's `referenced_ids` because LLMs hallucinate connected IDs (like Sales Orders)
            # that were not actually part of the SQL response.
            final_referenced_ids = auto_ids
        else:
            answer = "The query executed successfully but returned no results."
            final_referenced_ids = auto_ids
    except Exception as e:
        answer = f"Query executed successfully with {len(data)} results, but could not generate summary: {str(e)}"
        final_referenced_ids = auto_ids
    
    logger.info(f"Final answer: {answer}")
    
    return ChatResponse(
        answer=answer,
        sql=sql,
        explanation=explanation,
        data=data,
        referenced_ids=final_referenced_ids,
        row_count=len(data)
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)