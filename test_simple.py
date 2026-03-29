import asyncio
import aiohttp
from dotenv import load_dotenv
import psycopg
import os
import pandas as pd

load_dotenv()

async def extract_data(url: str) -> list[dict]:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                print("Траблы extract: {response.status}")
                return []
            return await response.json()

def transform_data(raw_data: list[dict]) -> list[dict]:
    df = pd.DataFrame(raw_data)
    
    df = df.dropna(subset=['title'])
    
    df['title'] = df['title'].str.strip().str.capitalize()
    
    df = df.rename(columns={'id': 'post_id', 'userId': 'user_id'})
    
    df = df[['post_id', 'title', 'user_id']]
    
    records = list(df.itertuples(index=False, name=None))
    
    return records

async def load_to_db(records: list[dict]):
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