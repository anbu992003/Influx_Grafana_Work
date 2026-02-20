#pip install duckdb langchain langchain-community langgraph sentence-transformers

import duckdb
from langchain_community.vectorstores import DuckDB
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import ChatOpenAI # Or any local LLM provider

# 1. Setup DuckDB Vector Store
embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
vectorstore = DuckDB(
    connection=duckdb.connect("rag_storage.db"), 
    embedding=embeddings, 
    table_name="embeddings"
)

# 2. Add some data (only needs to be done once)
vectorstore.add_texts(["DuckDB is a fast analytical database.", "SQLite is great for edge computing."])

# 3. Define the RAG Chain
retriever = vectorstore.as_retriever(search_kwargs={"k": 1})
template = """Answer the question based only on the following context:
{context}

Question: {question}
"""
prompt = ChatPromptTemplate.from_template(template)
llm = ChatOpenAI(model="gpt-4o-mini") # Swap for a local model if preferred

# The "Chain"
rag_chain = (
    {"context": retriever, "question": RunnablePassthrough()}
    | prompt
    | llm
)

# Execute
print(rag_chain.invoke("What is DuckDB?").content)
