from mcp.client import Client
import asyncio

async def test_add():
    # Create a client connected to the server (in-memory for local testing)
    async with Client("stdio://") as client:
        # List available tools
        tools = await client.list_tools()
        print("Available tools:", [tool.name for tool in tools.tools])

        # Call the 'add' tool
        result = await client.call_tool("add", {"a": 7, "b": 5})
        print("Result:", result.content)  # Outputs: 12

if __name__ == "__main__":
    asyncio.run(test_add())
