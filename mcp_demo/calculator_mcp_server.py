from mcp.server.fastmcp import FastMCP

# Initialize the MCP server with a name (this appears in LLM clients)
mcp = FastMCP("Calculator Server")

# Define a tool: A simple function decorated with @mcp.tool()
# FastMCP auto-generates schema from type hints (int for a, b) and docstring
@mcp.tool()
def add(a: int, b: int) -> int:
    """
    Adds two integers and returns the sum.
    
    Args:
        a: First integer
        b: Second integer
    
    Returns:
        The sum of a and b.
    """
    return a + b

# Run the server (uses stdio transport by default for local testing)
if __name__ == "__main__":
    mcp.run(transport="stdio")
