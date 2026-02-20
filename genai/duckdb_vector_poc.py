import duckdb
import numpy as np
from sentence_transformers import SentenceTransformer

# 1. Initialize the embedding model (runs locally)
model = SentenceTransformer('all-MiniLM-L6-v2') 

# 2. Setup DuckDB and the VSS (Vector Similarity Search) extension
con = duckdb.connect("my_rag_db.db")
con.execute("INSTALL vss;")
con.execute("LOAD vss;")

# 3. Create a table to store our documents and their embeddings
# all-MiniLM-L6-v2 produces vectors with 384 dimensions
con.execute("""
    CREATE TABLE IF NOT EXISTS documents (
        content TEXT,
        vec FLOAT[384]
    );
""")

# 4. Some sample data for our "Knowledge Base"
documents = [
    "DuckDB is an in-process SQL OLAP database management system.",
    "SQLite is a C-language library that implements a small, fast SQL database engine.",
    "RAG stands for Retrieval-Augmented Generation.",
    "Python is a high-level, general-purpose programming language."
]

# Generate embeddings and insert into DuckDB
for doc in documents:
    embedding = model.encode(doc).tolist()
    con.execute("INSERT INTO documents VALUES (?, ?)", [doc, embedding])

# 5. The Search Function
def search(query, limit=1):
    query_vec = model.encode(query).tolist()
    
    # Use array_distance for Cosine Similarity (lower is closer)
    result = con.execute(f"""
        SELECT content, array_distance(vec, ?::FLOAT[384]) as distance
        FROM documents
        ORDER BY distance ASC
        LIMIT {limit}
    """, [query_vec]).fetchall()
    
    return result

# 6. Test it out!
query_text = "Tell me about local databases"
matches = search(query_text)

print(f"Query: {query_text}")
for content, dist in matches:
    print(f"Match: {content} (Distance: {dist:.4f})")
