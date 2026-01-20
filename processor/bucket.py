import aiohttp
import asyncio
from config import BUCKET_NAME, FILE_NAME, get_supabase_config

async def download_from_bucket_async(session, bucket_name=BUCKET_NAME, file_name=FILE_NAME):
    url, key = get_supabase_config()
    if not url or not key:
        print("[ERROR] Configuração Supabase inválida.")
        return None

    storage_url = f"{url}/storage/v1/object/{bucket_name}/{file_name}"
    headers = {"Authorization": f"Bearer {key}", "apikey": key}

    async with session.get(storage_url, headers=headers) as resp:
        if resp.status == 404:
            return None
        if resp.status in (400, 401, 403):
            storage_url = f"{url}/storage/v1/object/public/{bucket_name}/{file_name}"
            async with session.get(storage_url) as resp_public:
                if resp_public.status != 200:
                    print(f"[ERROR] Download público falhou: {resp_public.status}")
                    return None
                return await resp_public.read()
        if resp.status != 200:
            print(f"[ERROR] Download falhou: {resp.status}")
            return None
        return await resp.read()

async def poll_bucket_async(interval=60):
    last_hash = None
    async with aiohttp.ClientSession() as session:
        while True:
            content = await download_from_bucket_async(session)
            if content:
                new_hash = hash(content)
                if new_hash != last_hash:
                    last_hash = new_hash
                    yield content
            await asyncio.sleep(interval)
