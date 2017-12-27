from flask import Flask, jsonify, request
from decimal import Decimal
import pytz
import datetime
import pymongo
import dateutil.parser

app = Flask(__name__)

mongo_client = pymongo.MongoClient('localhost', connect=False)
db = mongo_client.gdax_data

collection_map = {'BTC-USD': db.btc_usd,
                  'BCH-USD': db.bch_usd,
                  'ETH-USD': db.eth_usd,
                  'LTC-USD': db.ltc_usd,
                  'ETH-BTC': db.eth_btc,
                  'LTC-BTC': db.ltc_btc}


def datettime_to_epoch(date_in):
    epoch = datetime.datetime.fromtimestamp(0, pytz.utc)
    diff = date_in - epoch
    return int(diff.total_seconds())


@app.route('/products/<product_id>/candles/')
def get_historic_data(product_id):
    granularity = int(request.args.get('granularity', ''))

    start = request.args.get('start', '')
    start = dateutil.parser.parse(start)
    start = start - datetime.timedelta(minutes=start.minute % granularity, seconds=start.second, microseconds=start.microsecond)
    start = start.isoformat()

    end = str(request.args.get('end', ''))

    cur_time = dateutil.parser.parse(start)
    cur_time = cur_time.replace(tzinfo=pytz.utc)

    ret = collection_map[product_id].find({'time': {'$gte': start, '$lt': end}}, {'_id': False}).sort('time', 1)

    ret_list = []
    open_price = None
    high_price = None
    low_price = None
    close_price = None
    volume = Decimal('0.0')
    for doc in ret:
        if dateutil.parser.parse(doc.get('time')) > cur_time + datetime.timedelta(minutes=granularity):
            if close_price:
                ret_list.insert(0, [datettime_to_epoch(cur_time), low_price, high_price, open_price, close_price, volume])
            while dateutil.parser.parse(doc.get('time')) > cur_time + datetime.timedelta(minutes=granularity):
                cur_time = cur_time + datetime.timedelta(minutes=granularity)
            cur_time = cur_time - datetime.timedelta(minutes=granularity)
            open_price = None
            high_price = None
            low_price = None
            close_price = None
            volume = Decimal('0.0')
            cur_time = cur_time + datetime.timedelta(minutes=granularity)
        if not open_price:
            open_price = Decimal(doc.get('price'))
        if not high_price or Decimal(doc.get('price')) > high_price:
            high_price = Decimal(doc.get('price'))
        if not low_price or Decimal(doc.get('price')) < low_price:
            low_price = Decimal(doc.get('price'))
        close_price = Decimal(doc.get('price'))
        volume += Decimal(doc.get('size'))
    ret_list.insert(0, [datettime_to_epoch(cur_time), low_price, high_price, open_price, close_price, volume])

    return jsonify(ret_list)
