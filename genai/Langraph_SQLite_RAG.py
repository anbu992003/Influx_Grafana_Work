import sqlite3
import sqlite_vec
from typing import TypedDict, List, Optional
from langgraph.graph import StateGraph, START, END
from sentence_transformers import SentenceTransformer
from langchain_openai import ChatOpenAI

# 1. State Definition
class GraphState(TypedDict):
    question: str
    context: str
    distance: float
    retries: int
    answer: str

# 2. Setup Tools (SQLite & Model)
model = SentenceTransformer('all-MiniLM-L6-v2')
llm = ChatOpenAI(model="gpt-4o-mini")
DB_PATH = "local_rag.db"
DISTANCE_THRESHOLD = 1.1  # Adjust based on your model; lower is stricter

# 3. Define the Nodes
def retrieve(state: GraphState):
    """Searches SQLite for the best match."""
    print(f"--- RETRIEVING for: {state['question']} ---")
    query_vec = model.encode(state['question']).tolist()
    
    db = sqlite3.connect(DB_PATH)
    sqlite_vec.load(db)
    
    # Query the virtual table (vec0) for the nearest neighbor
    result = db.execute("""
        SELECT tc.content, vd.distance
        FROM vec_documents vd
        JOIN text_content tc ON vd.rowid = tc.id
        WHERE embedding MATCH ?
        ORDER BY distance LIMIT 1
    """, (sqlite_vec.serialize_float32(query_vec),)).fetchone()
    db.close()

    if result:
        return {"context": result[0], "distance": result[1]}
    return {"context": "No match found", "distance": 99.0}

def grade_search(state: GraphState):
    """Determines if the search result is 'good enough' or needs a retry."""
    print(f"--- GRADING (Distance: {state['distance']:.4f}) ---")
    
    if state["distance"] <= DISTANCE_THRESHOLD:
        return "useful"
    elif state.get("retries", 0) >= 1:
        return "too_many_retries"
    else:
        return "not_useful"

def rewrite_query(state: GraphState):
    """If the match was poor, ask the LLM to rephrase the question for a better search."""
    print("--- REWRITING QUERY ---")
    current_retries = state.get("retries", 0)
    
    msg = llm.invoke(f"Rephrase this search query to be more specific for a database search: {state['question']}")
    return {"question": msg.content, "retries": current_retries + 1}

def generate(state: GraphState):
    """Final node to produce the answer."""
    print("--- GENERATING FINAL ANSWER ---")
    response = llm.invoke(f"Context: {state['context']}\n\nQuestion: {state['question']}")
    return {"answer": response.content}

# 4. Build the Graph
workflow = StateGraph(GraphState)

workflow.add_node("retrieve", retrieve)
workflow.add_node("rewrite_query", rewrite_query)
workflow.add_node("generate", generate)

workflow.add_edge(START, "retrieve")

# Conditional Routing
workflow.add_conditional_edges(
    "retrieve",
    grade_search,
    {
        "useful": "generate",
        "not_useful": "rewrite_query",
        "too_many_retries": "generate" # Stop looping and just try with what we have
    }
)

workflow.add_edge("rewrite_query", "retrieve") # Loop back to search
workflow.add_edge("generate", END)

app = workflow.compile()

# 5. Execute
# Try a query that exists: "Tell me about SQLite"
# Try a query that might fail: "Who won the 1994 world cup?"
result = app.invoke({"question": "Tell me about SQLite", "retries": 0})
print(f"\nFINAL ANSWER:\n{result['answer']}")
