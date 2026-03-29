import asyncio
import aiohttp
from dotenv import load_dotenv
import psycopg
import os

load_dotenv()

async def extract_data(url: str) -> list[dict]:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                print("Траблы extract: {response.status}")
                return []
            return await response.json()

def transform_data(raw_data: list[dict]) -> list[dict]:
    cleaned = []
    for item in raw_data:
        if not item.get('title'):
            continue
        cleaned.append({
            "post_id": item['id'],
            'title': item['title'].strip().capitalize(),
            'user_id': item['userId']
        })
    return cleaned

async def load_to_db(data: list[dict]):
    conn_info = f"dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} host={os.getenv('DB_HOST')} port={os.getenv('DB_PORT')}"
    async with await psycopg.AsyncConnection.connect(conn_info) as conn:
        
        async with conn.cursor() as cur:
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS etl_posts (
                    post_id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    user_id INTEGER NOT NULL
                );
            """)

            records = [(item['post_id'], item['title'],item['user_id']) for item in data]
            
            await cur.executemany("""
                INSERT INTO etl_posts (post_id, title, user_id) 
                VALUES (%s, %s, %s)
                ON CONFLICT (post_id) DO NOTHING;
            """, records)
            
            await conn.commit()
            
async def main():
    url = "https://jsonplaceholder.typicode.com/posts"
    print('начинаю ETL процесс.')
    raw_data = await extract_data(url)
    if raw_data:
        clean = transform_data(raw_data)
        await load_to_db(clean)
    else:
        print('НЕТУ ДАННЫХ ДЛЯ ОБРАБОТКИ.')
if __name__ == "__main__":
    # все тот же фикс от калловой винды cнизу
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())