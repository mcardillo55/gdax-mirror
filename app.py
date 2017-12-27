from flask import Flask, jsonify, request
from decimal import Decimal
import datetime
import pymongo
import dateutil.parser

app = Flask(__name__)

mongo_client = pymongo.MongoClient('mongoserver')
db = mongo_client.gdax_data

collection_map = {'BTC-USD': db.btc_usd,
                  'BCH-USD': db.bch_usd,
                  'ETH-USD': db.eth_usd,
                  'LTC-USD': db.ltc_usd,
                  'ETH-BTC': db.eth_btc,
                  'LTC-BTC': db.ltc_btc}


@app.route('/products/<product_id>/candles/')
def get_historic_data(product_id):
    start = str(request.args.get('start', ''))
    end = str(request.args.get('end', ''))
    print(start)
    print(end)
    granularity = int(request.args.get('granularity', ''))

    cur_time = dateutil.parser.parse(start)

    ret = collection_map[product_id].find({'time': {'$gte': start, '$lt': end}}, {'_id': False})

    ret_list = []
    open_price = None
    high_price = None
    low_price = None
    close_price = None
    volume = Decimal('0.0')
    for doc in ret:
        if dateutil.parser.parse(doc.get('time')) > cur_time + datetime.timedelta(minutes=granularity):
            ret_list.append([int(cur_time.timestamp()), str(low_price), str(high_price), str(open_price), str(close_price), str(volume)])
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
    ret_list.append([int(cur_time.timestamp()), str(low_price), str(high_price), str(open_price), str(close_price), str(volume)])

    return jsonify(ret_list)