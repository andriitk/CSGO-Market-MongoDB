import asyncio
from database.redis_con import add_data_to_redis, get_data
import motor.motor_asyncio
from database.mongo_con import only_check_stock_in_on_sale, only_insert_data_on_sale, only_check_stock_in_history, \
    only_insert_data_history

import datetime
import requests
import logging


# Func for insert of add market hash names ro redis DB
def insert_market_hash_names():
    response = requests.get(
        "https://api.steamapis.com/market/items/730?api_key=r39OfExGTYD64Bf8MGntG8oaJgQ&format=comact", timeout=30)
    data = response.json()

    market_hash_names = get_data(table='market_hash_names', db=0)

    if not market_hash_names:
        market_hash_names = {"created_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                             'market_hash_names': {}}
        for key, value in data.items():
            market_hash_names['market_hash_names'][key] = str(value)
        add_data_to_redis(table='market_hash_names', data=market_hash_names, db=0)
    else:
        for key, value in data.items():
            market_hash_names['market_hash_names'][key] = value

    return market_hash_names


async def insert_on_sales(data: dict):
    client_on_sale = motor.motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017")
    db = client_on_sale["market_csgo"]

    on_sale = db.csgo_on_sale
    on_sale.create_index([('market_id', 1)], unique=True)

    not_founds = 0
    need_checks = 0
    on_sale_status = 0

    async with await client_on_sale.start_session() as session:
        market_ids = {result["market_id"] for result in await on_sale.find({}, {"market_id": 1}).to_list(length=None)}

        if market_ids:
            diff_keys = market_ids - {*data.keys()}

            tasks_first = [
                asyncio.create_task(only_check_stock_in_on_sale(on_sale, key, not_founds, need_checks, session))
                for key in diff_keys]
            await asyncio.gather(*tasks_first, return_exceptions=True)

        tasks_second = [only_insert_data_on_sale(on_sale, market_id, value, on_sale_status, session) for
                        market_id, value in data.items()]
        await asyncio.gather(*tasks_second, return_exceptions=True)

        if on_sale_status:
            print(f"{on_sale_status} elements got 'on_sale' status in ON_SALE DB.")


async def insert_history(data: dict):
    client_history = motor.motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017")
    db = client_history["market_csgo"]
    history = db.csgo_history
    history.create_index([('market_hash_name', 1)], unique=True)

    not_founds = 0

    async with await client_history.start_session() as session:
        market_hash_names = {result['market_hash_name'] for result in
                             await history.find({}, {'market_hash_name': 1}).to_list(length=None)}

        if market_hash_names:
            diff_keys = market_hash_names - {*data.keys()}

            tasks_first = [only_check_stock_in_history(history, key, not_founds, session) for key
                           in diff_keys]
            await asyncio.gather(*tasks_first, return_exceptions=True)

        if not_founds:
            logging.info(f"{not_founds} elements got 'not_found' status in HISTORY DB.")
            print(f"{not_founds} elements got 'not_found' status in HISTORY DB\n")

        tasks_second = [only_insert_data_history(history, market_hash_name, value, session) for
                        market_hash_name, value in data.items()]
        await asyncio.gather(*tasks_second, return_exceptions=True)


async def compare_data():
    client_compare = motor.motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017")
    db = client_compare["market_csgo"]

    on_sale = db.csgo_on_sale
    history = db.csgo_history

    fourth_stage = db.csgo_fourth_stage
    fourth_stage.create_index([('market_id', 1)], unique=True)

    async with await client_compare.start_session() as session:
        print("\n\n-------------------4 STAGE---------------------")
        data_on_sale = await on_sale.find({}, session=session).to_list(length=None)
        data_history = await history.find({}, session=session).to_list(length=None)

        dict_on_sale = {(item['market_hash_name'], item['price'], item['status']): {"market_id": item['market_id'],
                                                                                    'asset': item['asset'],
                                                                                    'class_id': item['class_id'],
                                                                                    'instance_id': item['instance_id']}
                        for item in data_on_sale}

        dict_history = {(item['market_hash_name'], i, item['status']): {"market_hash_name": item['market_hash_name']}
                        for item in data_history for i in item['price']}

        list_of_sales = [i for i in dict_history.keys() if i in dict_on_sale.keys()]

        count = 0
        for i in list_of_sales:
            if not await fourth_stage.find_one({"market_id": dict_on_sale[i]['market_id']}, session=session):
                stmt = {
                    'market_id': dict_on_sale[i]['market_id'],
                    'market_hash_name': i[0],
                    'price': i[1],
                    'status': 'found',
                    'asset': dict_on_sale[i]['asset'],
                    'class_id': dict_on_sale[i]['class_id'],
                    'instance_id': dict_on_sale[i]['instance_id'],
                    'created_at': datetime.datetime.now()
                }
                count += 1
                await fourth_stage.update_one({"market_id": dict_on_sale[i]['market_id']}, {"$set": stmt},
                                              session=session, upsert=True)

                logging.info(f"[+] Found match element with name: '{i[0]}'; price: {i[1]}.")
                print(f"[+] Found match element with name: '{i[0]}'; price: {i[1]}.")

        if count:
            logging.info(
                f"Found {count} new elements will be on sales. Relevant column added to History table.")
            print(
                f"\n[!] Found {count} new elements will be on sales. Relevant column added to History table.\n")
        else:
            logging.info("Not found matching elements. Stage 4 finished without changes.")
            print("[-] Not found matching elements. Stage 4 finished without changes.\n")
