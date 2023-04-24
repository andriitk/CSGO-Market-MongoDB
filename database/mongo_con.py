import motor.motor_asyncio
import datetime
import logging


async def only_check_stock_in_on_sale(on_sale, key: str, not_founds: int, need_checks: int, session):
    query = await on_sale.find_one({"market_id": key}, session=session)
    time_db = query["created_at"].timestamp()
    time_now = datetime.datetime.now().timestamp()

    if time_now - time_db > 3600:
        stmt = {"$set": {"status": "not_found", "created_at": datetime.datetime.now()}}
        await on_sale.update_one({"market_id": key}, stmt, session=session)
        not_founds += 1
    else:
        stmt = {"$set": {"status": "need_check", "created_at": datetime.datetime.now()}}
        await on_sale.update_one({"market_id": key}, stmt, session=session)
        need_checks += 1


async def only_insert_data_on_sale(on_sale, market_id, value, on_sale_status: int, session):
    exists_query = await on_sale.find_one({"market_id": market_id}, session=session)

    if exists_query:
        status = exists_query["status"]

        if status == "not_found":
            stmt_1 = {"$set": {
                "asset": value['asset'],
                "price": value['price'],
                "created_at": datetime.datetime.now(),
                "status": "on_sale"
            }}
            await on_sale.update_one({"market_id": exists_query["market_id"]}, stmt_1, session=session)
            on_sale_status += 1
        else:
            stmt_1 = {"$set": {
                "created_at": datetime.datetime.now(),
                "asset": value['asset'],
                "price": value['price'],
            }}
            await on_sale.update_one({"market_id": exists_query["market_id"]}, stmt_1, session=session)
            logging.info(f"Update existing record in the ON_SALE '{value['market_hash_name']} : {market_id}'")
    else:
        value['market_id'] = market_id
        await on_sale.insert_one(value, session=session)
        logging.info(f"Insert new record to ON_SALE DB '{value['market_hash_name']} : {market_id}'.")


async def only_check_stock_in_history(history, key: str, not_founds: int, session):
    query = await history.find_one({'market_hash_name': key}, session=session)
    time_db = query['created_at'].timestamp()
    time_now = datetime.datetime.now().timestamp()

    if time_now - time_db > 3600:
        stmt = {'$set': {
            'status': 'not_found',
            'created_at': datetime.datetime.now()}}
        await history.update_one({'market_hash_name': key}, stmt, session=session)
        not_founds += 1


async def only_insert_data_history(history, market_hash_name, value, session):
    exists_query = await history.find_one({'market_hash_name': market_hash_name}, session=session)

    if not exists_query:
        stmt_1 = {'market_hash_name': market_hash_name,
                  'time': value['time'],
                  'price': value['price'],
                  "status": "need_check",
                  "created_at": datetime.datetime.now()}

        await history.insert_one(stmt_1, session=session)
        logging.info(f"Insert new record to HISTORY DB '{market_hash_name}'.")
    else:
        prices = exists_query['price']
        times = exists_query['time']

        stmt_1 = {'$set': {'price': list(set(prices) | set(value['price'])),
                           'time': list(set(times) | set(value['time'])),
                           'created_at': datetime.datetime.now()}}
        await history.update_one({'market_hash_name': exists_query['market_hash_name']}, stmt_1, session=session)
        logging.info(f"Update existing record in the HISTORY DB with name '{market_hash_name}'.")


async def count_elements_on_sale():
    client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017")
    db = client["market_csgo"]
    on_sale = db.csgo_on_sale

    return await on_sale.count_documents({})


async def count_elements_history():
    client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017")
    db = client["market_csgo"]
    history = db.csgo_history

    return await history.count_documents({})
