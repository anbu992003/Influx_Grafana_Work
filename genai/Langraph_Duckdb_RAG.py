#pip install duckdb langchain langchain-community langgraph sentence-transformers

from typing import TypedDict, List
from langgraph.graph import StateGraph, START, END

# 1. Define the State
class GraphState(TypedDict):
    question: str
    context: str
    answer: str

# 2. Define the Nodes
def retrieve(state: GraphState):
    # Uses the retriever from the LangChain example above
    docs = retriever.invoke(state["question"])
    return {"context": "\n".join([d.page_content for d in docs])}

def generate(state: GraphState):
    # Formats and calls the LLM
    response = llm.invoke(f"Context: {state['context']}\n\nQuestion: {state['question']}")
    return {"answer": response.content}

# 3. Build the Graph
workflow = StateGraph(GraphState)

# Add Nodes
workflow.add_node("retrieve_node", retrieve)
workflow.add_node("generate_node", generate)

# Define Flow (Edges)
workflow.add_edge(START, "retrieve_node")
workflow.add_edge("retrieve_node", "generate_node")
workflow.add_edge("generate_node", END)

# Compile
app = workflow.compile()

# Execute
result = app.invoke({"question": "Tell me about SQLite"})
print(result["answer"])
