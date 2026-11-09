"""What a computer that joined with a lend link is given to compute.

Code that someone typed runs with the lending computer's account and files, and a
file search reads the shared folder, which a lender may not see. So those jobs go
to your own devices only, while the built-in kernels still use lenders.
"""

import asyncio

from conftest import FakeBrowser


def test_typed_code_and_searches_skip_lenders(fresh_coord):
    c = fresh_coord.client()
    path = fresh_coord.tmp / "sample.txt"
    path.write_bytes(b"error ok error\n" * 20000)
    c.upload(path)

    async def main():
        async with FakeBrowser(fresh_coord, "Owner tab", threads=2) as owner, \
                FakeBrowser(fresh_coord, "Friend tab", threads=2, token=fresh_coord.lend_token) as friend:
            owner.file_token = c.token
            await owner.wait_for(lambda m: m["type"] == "welcome")
            await friend.wait_for(lambda m: m["type"] == "welcome")

            job = await owner.run("custom", {"code": "function run(c){return c}", "chunks": 40, "combine": "list", "lang": "js"})
            assert job["status"] == "done"
            job = await owner.run("grep", {"file": "sample.txt", "term": "error", "chunkMB": 1})
            assert job["status"] == "done", job
            assert job["result"]["count"] == 40000
            assert not [m for m in friend.msgs if m["type"] == "chunk" and m["kind"] in ("custom", "pymap", "grep")]

            # built-in kernels are still lent: the friend's tab takes part in a prime count
            job = await owner.run("primes", {"limit": 2 * 10**8, "chunkSize": 10**6})
            assert job["status"] == "done"
            assert friend.computed > 0

    asyncio.run(main())
