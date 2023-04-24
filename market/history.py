import asyncio
import aiohttp
import urllib.parse
from database.services_new import insert_history
from database.mongo_con import count_elements_history
import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S', filename='market_hash_names.log',
    filemode='a')


async def fetch_data_history(data_history: dict, key, hash_chunk, session):
    tail = ''
    for block in hash_chunk:
        tail += '&list_hash_name[]=' + urllib.parse.quote(block[0])
    url = f"https://market.csgo.com/api/v2/get-list-items-info?key={key}{tail}"

    try:
        async with session.get(url) as response:
            status_code = response.status
            data = await response.json()

            for name, items in data['data'].items():
                prices = set()
                for block in items['history']:
                    if "." not in str(block[1]):
                        prices.add(f"{block[1]}.0")
                    else:
                        prices.add(str(block[1]))

                data_history[name] = {
                    "time": [str(datetime.datetime.fromtimestamp(int(block[0]))) for block in items['history']],
                    "price": list(prices),
                    "status": "need_check",
                    "created_at": datetime.datetime.now()}

    except aiohttp.client_exceptions.ContentTypeError as ex:
        print(f"[!] Exception - HISTORY: ConnectError. Status code: {status_code}")
        logging.error(
            f"[!] Exception - HISTORY - ConnectError. "
            f"Skip {len(hash_chunk)} chunks in HISTORY stage. Status code: {status_code}")

    except Exception as ex:
        print(f"[!] Exception - HISTORY: {ex}. Status code: {status_code}")
        logging.error(
            f"[!] Exception - HISTORY - {ex}. "
            f"Skip {len(hash_chunk)} chunks in HISTORY stage. Status code: {status_code}")


# ASYNCIO
async def main_history(hash_chunks: list, data_history: dict):
    async with aiohttp.ClientSession() as session:
        history_tasks = [asyncio.create_task(
            fetch_data_history(data_history=data_history, key=hash_chunk[0], hash_chunk=hash_chunk[1],
                               session=session)) for index, hash_chunk in enumerate(hash_chunks)]

        await asyncio.gather(*history_tasks, return_exceptions=True)

    logging.info("Start inserted data to HISTORY DB")
    print("Start inserted data to HISTORY DB")

    await insert_history(data=data_history)

    logging.info(f"Amount elements on all iterations in HISTORY stage: {len(data_history)}")
    logging.info(f"Now HISTORY DB is storing: {await count_elements_history()} records")

    print(f"Amount elements on all iterations in HISTORY stage: {len(data_history)}")
    print(f"Now HISTORY DB is storing: {await count_elements_history()} records")
