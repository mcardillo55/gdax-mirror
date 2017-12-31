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
    start = dateutil.parser.parse(request.args.get('start', ''))
    start = start.replace(tzinfo=pytz.utc)
    end = dateutil.parser.parse(request.args.get('end', ''))
    end = end.replace(tzinfo=pytz.utc)

    cur_end = end
    cur_end = cur_end.replace(tzinfo=pytz.utc)
    cur_start = cur_end - datetime.timedelta(minutes=cur_end.minute % granularity, seconds=cur_end.second, microseconds=cur_end.microsecond)

    ret_list = []

    while cur_start > start:
        total_volume = Decimal('0.0')
        pipeline = [
            {'$match': {'time': {'$gte': cur_start.isoformat(), '$lt': cur_end.isoformat()}}},
            {'$sort': {'time': 1}},
            {'$group': {'_id': None, 'open': {'$first': '$price'}, 'high': {'$max': '$price'}, 'low': {'$min': '$price'}, 'close': {'$last': '$price'}}}
        ]
        try:
            ret = next(collection_map[product_id].aggregate(pipeline))
        except StopIteration:
            pass

        vol_ret = collection_map[product_id].find({'time': {'$gte': cur_start.isoformat(), '$lt': cur_end.isoformat()}}, {'_id': 0, 'size': 1})
        for vol_record in vol_ret:
            total_volume += Decimal(vol_record['size'])

        if total_volume > Decimal('0.0'):
            ret_list.append([datettime_to_epoch(cur_start), Decimal(ret['low']), Decimal(ret['high']), Decimal(ret['open']), Decimal(ret['close']), total_volume])

        cur_start = cur_start - datetime.timedelta(minutes=granularity)
        cur_end = cur_start + datetime.timedelta(minutes=granularity) - datetime.timedelta(microseconds=1)

    return jsonify(ret_list)
