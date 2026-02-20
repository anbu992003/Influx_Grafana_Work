#pip install sqlite-vec sentence-transformers

import sqlite3
import sqlite_vec
from sentence_transformers import SentenceTransformer

# 1. Initialize the embedding model (384 dimensions)
model = SentenceTransformer('all-MiniLM-L6-v2')

# 2. Setup SQLite and load the vector extension
# We use a file-based DB, but ":memory:" also works for testing
db = sqlite3.connect("local_rag.db")
db.enable_load_extension(True)
sqlite_vec.load(db)
db.enable_load_extension(False)

# 3. Create a Virtual Table for vectors
# 'vec0' is the specialized table type provided by sqlite-vec
db.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS vec_documents USING vec0(
        embedding float[384]
    );
""")

# Also create a standard table to hold the actual text (linked by rowid)
db.execute("""
    CREATE TABLE IF NOT EXISTS text_content (
        id INTEGER PRIMARY KEY,
        content TEXT
    );
""")

# 4. Data to Index
kb_articles = [
    "The capital of France is Paris.",
    "SQLite is a file-based database engine.",
    "The speed of light is approximately 299,792,458 meters per second.",
    "Python was created by Guido van Rossum."
]

# Embed and Insert
for text in kb_articles:
    vec = model.encode(text).tolist()
    
    # Insert text into the normal table
    cursor = db.cursor()
    cursor.execute("INSERT INTO text_content (content) VALUES (?)", (text,))
    row_id = cursor.lastrowid
    
    # Insert vector into the virtual table using the same rowid
    db.execute(
        "INSERT INTO vec_documents(rowid, embedding) VALUES (?, ?)",
        (row_id, sqlite_vec.serialize_float32(vec))
    )
db.commit()

# 5. The Search Function
def perform_rag_search(query_text, limit=1):
    # Convert query to vector
    query_vec = model.encode(query_text).tolist()
    
    # Perform Vector Search
    # We join the virtual vector table with our text table
    results = db.execute(f"""
        SELECT 
            text_content.content,
            vec_documents.distance
        FROM vec_documents
        LEFT JOIN text_content ON vec_documents.rowid = text_content.id
        WHERE embedding MATCH ?
        ORDER BY distance
        LIMIT {limit}
    """, (sqlite_vec.serialize_float32(query_vec),)).fetchall()
    
    return results

# 6. Test it
query = "Tell me about French geography"
matches = perform_rag_search(query)

print(f"Query: {query}")
for content, distance in matches:
    print(f"Retrieved Context: {content} (Distance: {distance:.4f})")

db.close()
