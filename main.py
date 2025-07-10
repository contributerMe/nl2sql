import os
import sqlite3
import pandas as pd
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

EXCEL_DIR = "school_data"
DB_NAME = "excel_data.db"
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# Initialize the client
client = OpenAI(
    api_key=GROQ_API_KEY,
    base_url="https://api.groq.com/openai/v1"
)

def stdize_table_name(filename):
    """Standardize table names by removing spaces and hyphens"""
    name = os.path.splitext(filename)[0]
    return name.replace(" ", "_").replace("-", "_")

def make_db(dir, db_name):
    """Create SQLite database from Excel files"""
    conn = sqlite3.connect(db_name)
    for filename in os.listdir(dir):
        if filename.endswith(".xlsx"):
            file_path = os.path.join(dir, filename)
            table_name = stdize_table_name(filename)
            try:
                df = pd.read_excel(file_path)
                df.to_sql(table_name, conn, if_exists="replace", index=False)
                print(f"Loaded '{filename}' as table '{table_name}'")
            except Exception as e:
                print(f"Error loading {filename}: {e}")
    conn.close()

def get_db_schema(db_path):
    """Get complete database schema with table and column information"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    schema = []
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    for (table,) in tables:
        cursor.execute(f"PRAGMA table_info('{table}')")
        columns = cursor.fetchall()
        column_descriptions = [f"{col[1]} ({col[2]})" for col in columns]
        schema.append(f"Table: {table}\nColumns: {', '.join(column_descriptions)}\n")
    conn.close()
    return "\n".join(schema)

def get_table_sample(db_path, table_name, limit=3):
    """Get sample rows from a table"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM '{table_name}' LIMIT {limit}")
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    conn.close()
    return columns, rows

def identify_relevant_schema(schema_str, user_question):
    """
    First LLM call: Identify which tables and columns are relevant
    to answer the user's question.
    """
    system_prompt = """You are a database analyst that identifies which tables and columns 
    are needed to answer a user's question. Return ONLY a JSON format response with:
    - "tables": List of relevant table names
    - "columns": Dictionary with table names as keys and lists of relevant columns as values
    - "reasoning": Brief explanation of why these elements were selected"""

    user_prompt = f"""Given the following database schema:
    {schema_str}

    Identify which tables and columns are needed to answer this question:
    \"{user_question}\""""

    response = client.chat.completions.create(
        model="llama3-8b-8192",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.3,
        max_tokens=500
    )
    
    try:
        return eval(response.choices[0].message.content)
    except:
        # Fallback if JSON parsing fails
        return {
            "tables": [],
            "columns": {},
            "reasoning": "Failed to parse response"
        }

def generate_sql(schema_str, user_question, focused_schema):
    """
    Second LLM call: Generate SQL query using only the focused schema elements
    """
    system_prompt = """You are an expert SQL developer. Write a SQLite query that:
    1. Answers the user's question precisely
    2. Uses only the tables and columns provided in the focused schema
    3. Includes appropriate JOINs where needed
    4. Handles edge cases like NULL values
    5. Is optimized for performance

    All generated queries must be for the SQLite 3 dialect
    Return ONLY the SQL query, nothing else."""

    user_prompt = f"""Question to answer:
    \"{user_question}\"

    Relevant database elements:
    {focused_schema}

    Write a SQL query that answers the question using only these tables and columns.
    Include comments for complex operations if needed."""

    response = client.chat.completions.create(
        model="llama3-8b-8192",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0,
        max_tokens=500
    )
    return response.choices[0].message.content.strip()

def create_focused_schema(db_path, schema_info):
    """
    Create detailed description of relevant schema elements including:
    - Full column definitions
    - Sample data
    - Potential relationships
    """
    focused_schema = []
    reasoning = schema_info.get("reasoning", "No reasoning provided")
    
    focused_schema.append(f"Relevant elements needed because: {reasoning}\n")
    
    for table in schema_info.get("tables", []):
        # Get full column info
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info('{table}')")
        columns = cursor.fetchall()
        
        # Filter for relevant columns if specified
        relevant_cols = schema_info.get("columns", {}).get(table, [])
        if relevant_cols:
            columns = [col for col in columns if col[1] in relevant_cols]
        
        # Build column descriptions
        column_descriptions = []
        for col in columns:
            col_name, col_type = col[1], col[2]
            column_descriptions.append(f"{col_name} ({col_type})")
        
        # Get sample data
        sample_columns, sample_data = get_table_sample(db_path, table)
        
        # Build table description
        table_desc = [
            f"Table: {table}",
            f"Columns: {', '.join(column_descriptions)}",
            "Sample data:"
        ]
        
        # Add sample rows (only showing relevant columns if specified)
        for row in sample_data:
            if relevant_cols:
                # Only show values for relevant columns
                row_data = []
                for col in sample_columns:
                    if col in relevant_cols:
                        idx = sample_columns.index(col)
                        row_data.append(f"{col}={row[idx]}")
                table_desc.append(", ".join(row_data))
            else:
                table_desc.append(str(row))
        
        focused_schema.append("\n".join(table_desc))
    
    return "\n\n".join(focused_schema)

def run_query(db_path, sql_query):
    """Execute SQL query and return results"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        conn.close()
        return columns, rows
    except Exception as e:
        return None, str(e)

def get_ans(user_question, columns, result):
    """
    Converts SQL query results into a natural language answer for the user's question.
    
    Args:
        user_question (str): The original question asked by the user
        columns (list): List of column names from the query result
        result (list): List of tuples containing the query results
        
    Returns:
        str: Natural language answer summarizing the results
    """
    system_prompt = """You are a helpful data analyst that explains SQL query results in natural language.
    Your task is to:
    1. Interpret the data results in the context of the original question
    2. Provide a clear, concise answer in plain English
    3. Include relevant numbers and facts from the results
    4. Add brief context if needed
    5. Keep the response professional but conversational
    
    The user will provide:
    - Their original question
    - The column names from the result
    - The actual result data
    
    Return ONLY the natural language response, nothing else."""
    
    # Format the results for the prompt
    if not result:
        results_str = "No results found"
    else:
        results_str = "\n".join([str(row) for row in result[:10]]) 
    
    user_prompt = f"""Original question: {user_question}
    
Result columns: {columns}
    
Result data:
{results_str}
    
Please provide a natural language answer to the original question based on these results."""
    
    response = client.chat.completions.create(
        model="llama3-8b-8192",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.3,  
        max_tokens=300
    )
    
    return response.choices[0].message.content.strip()
    

def main():

    print("Reading Excel files and building database...")
    make_db(EXCEL_DIR, DB_NAME)

    schema = get_db_schema(DB_NAME)
    print("\nExtracted Schema:\n", schema)

    while True:
        user_question = input("\nAsk your question (or 'quit' to exit): ").strip()
        if user_question.lower() == 'quit':
            break
            
        try:
            # Phase 1: Identify relevant schema elements
            print("\nAnalyzing which tables and columns are needed...")
            schema_info = identify_relevant_schema(schema, user_question)
            
            # Create focused schema description
            focused_schema = create_focused_schema(DB_NAME, schema_info)
            print("\nFocused Schema Elements:\n", focused_schema)
            
            # Phase 2: Generate SQL with focused context
            sql_query = generate_sql(schema, user_question, focused_schema)

             #remove `` from the start and end of the query
            sql_query = sql_query.strip("`")

            print("\nGenerated SQL:\n", sql_query)
            
            # Execute query
            columns, result = run_query(DB_NAME, sql_query)
            

            if isinstance(result, str):
                print("Error:", result)
            else:
                print("\nQuery Result:")
                print("Columns:", columns)
                final_res = get_ans(user_question, columns, result)

                print(final_res)

                for row in result[:10]: 
                    print(row)
                if len(result) > 10:
                    print(f"... ({len(result)-10} more rows)")
                    
        except Exception as e:
            print(f"Error: {str(e)}")


if __name__ == "__main__":
    main()