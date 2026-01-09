import asyncio
import fastmcp
import time

async def main():
    async with fastmcp.Client("http://localhost:8000/mcp") as client:
        res = await client.call_tool("start_session", {"cwd": "/"})
        pid = int(res.content[0].text)
        await client.call_tool("add_input", {"pid": pid, "addon": "for i in {1..120}; do echo tick $i; sleep 1; done\n"})

        offset = 0
        while True:
            res = await client.call_tool("get_output", {"pid": pid, "offset": offset})
            addon = res.content[0].text
            print(addon, end="", flush=True)
            offset += len(addon)
            time.sleep(0.1)

asyncio.run(main())
