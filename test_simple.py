import asyncio
import aiohttp
from dotenv import load_dotenv
import psycopg
import os
import pandas as pd
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import sys
import redis.asyncio as redis
import json
import aioboto3 
from datetime import datetime

load_dotenv()

REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = os.getenv('REDIS_PORT', 6379)
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')

# этот блок мог бы спокойно вынести в отдельный файл, позже эти займусь
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True
)

async def extract_data(url: str) -> list[dict]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()
    except aiohttp.ClientError as e:
        print('сетевая ошибка при запуске')
        return None

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

async def analytics():
    conn_info = f"dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} host={os.getenv('DB_HOST')} port={os.getenv('DB_PORT')}"
    async with await psycopg.AsyncConnection.connect(conn_info) as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS user_post_stats (  
                    user_id INTEGER PRIMARY KEY,  
                    post_count INTEGER  
                );  
            """)
            
            await cur.execute("""
                INSERT INTO user_post_stats (user_id, post_count)
                SELECT user_id, COUNT(post_id) 
                FROM etl_posts 
                GROUP BY user_id
                ON CONFLICT (user_id) DO UPDATE 
                SET post_count = EXCLUDED.post_count;
            """)
            await conn.commit()
            print('база аналитиков обновлена')

async def cons_worker():
    print('ЖДУ ДАННЫЕ ИЗ ОЧЕРЕДИ')
    while True:
        result = await redis_client.brpop('truba_ebat', timeout=0) #таймаут для вечного ожидания
        if result:
            truba_name, message_ot_trubi = result
            data = json.loads(message_ot_trubi)
            print('ДОСТАЛ БАТЧ, НАЧИНАЮ ОБРАБОТКУ')
            clean = transform_data(data)
            await load_to_db(clean)
            print('БАТЧ УЖЕ В БД')
            await analytics()

async def aps_time():
    url = "https://jsonplaceholder.typicode.com/posts"
    print('начинаю ETL процесс.')
    raw_data = await extract_data(url)
    if raw_data:
        await bucket_minio(raw_data)
        await redis_client.lpush('truba_ebat', json.dumps(raw_data))
        print('БАТЧ В ОБРАБОТКЕ')
    else:
        print('НЕТУ ДАННЫХ ДЛЯ ОБРАБОТКИ.')

async def bucket_minio(raw_data):
    
    endpoint = "http://minio:9000"
    minio_key = os.getenv("MINIO_USER")
    minio_secret_key = os.getenv("MINIO_PASSWORD")
    
    file_name = f"raw_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    session = aioboto3.Session()
    async with session.client('s3', endpoint_url=endpoint, aws_access_key_id=minio_key, aws_secret_access_key=minio_secret_key, region_name='us-east-1') as client:
        await client.put_object(
            Bucket='raw-data',
            Key=file_name,
            Body=json.dumps(raw_data)
        )
        print(f'СЫРОЙ БЕКАП СОХРАНЕН {file_name}')

async def main():
    
    scheduler = AsyncIOScheduler()
    
    scheduler.add_job(aps_time, 'interval', seconds=25, misfire_grace_time=10)
    
    scheduler.start()
    
    asyncio.create_task(cons_worker())
    
    print('оркестратор запущен')
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        pass

if __name__ == "__main__":
    # все тот же фикс от калловой винды cнизу
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())