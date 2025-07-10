import os
import sqlite3
import pandas as pd
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

EXCEL_DIR = "school_data"
DB_NAME = "excel_data.db"
GROQ_API_KEY = os.getenv("GROQ_API_KEY")


#initalize the client 
client = OpenAI(
    api_key = GROQ_API_KEY,
    base_url = "https://api.groq.com/openai/v1"
)

#removes hyphen and spaces from table name 
def stdize_table_name(filename):
    name = os.path.splitext(filename)[0]
    return name.replace(" ", "_").replace("-", "_")

#makes a sql database from excel files from the given folder
def make_db(dir, db_name):
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
                print(f"{e}")
    conn.close()

#get entire database schema
def get_db_schema(db_path):
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

def generate_sql(schema_str, user_question):

    system_prompt = "You are a helpful assistant that writes SQL queries for SQLite databases."

    user_prompt = f"""

Write a SQL query that answers the question:
\"{user_question}\"  

Given the following SQLite database schema:
{schema_str}



Only return the SQL query, and nothing else.
"""
    
    response = client.chat.completions.create(
        model="llama3-8b-8192",  
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0,
        max_tokens=100,
    )
    return response.choices[0].message.content.strip()

def run_query(db_path, sql_query):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        conn.close()
        return columns, rows
    except Exception as e:
        return None, "f{e}"

def main():

    print("Reading Excel files and building database...")
    make_db(EXCEL_DIR, DB_NAME)

    schema = get_db_schema(DB_NAME)
    print("\n Extracted Schema:\n", schema)

    while True:
        user_question = input("\n Ask your question: ")
        sql_query = generate_sql(schema, user_question)
        print("\n Generated SQL:\n", sql_query)

        columns, result = run_query(DB_NAME, sql_query)

        if isinstance(result, str):
            print("Error:", result)
        else:
            print("\nQuery Result:")
            print(columns)
            for row in result:
                print(row)

if __name__ == "__main__":
    main()
