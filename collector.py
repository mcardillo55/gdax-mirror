# import PyMongo and connect to a local, running Mongo instance
from pymongo import MongoClient
import dateutil.parser
import gdax
import time


class myWSClient(gdax.WebsocketClient):
    def __init__(self, url="wss://ws-feed.gdax.com", products=None, message_type="subscribe", mongo_collection=None,
                 should_print=True, auth=False, api_key="", api_secret="", api_passphrase="", channels=None):
        mongo_client = MongoClient('mongodb://localhost:27017/')
        # specify the database and collection
        db = mongo_client.gdax_data
        self.collection_map = {'BTC-USD': db.btc_usd,
                               'BCH-USD': db.bch_usd,
                               'ETH-USD': db.eth_usd,
                               'LTC-USD': db.ltc_usd,
                               'ETH-BTC': db.eth_btc,
                               'LTC-BTC': db.ltc_btc}
        super(myWSClient, self).__init__(url=url, products=products, message_type=message_type, mongo_collection=mongo_collection,
                                         should_print=should_print, auth=auth, api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase, channels=channels)

    def on_message(self, msg):
        if msg.get('type') == 'match':
            msg['time'] = dateutil.parser.parse(msg.get('time'))
            self.collection_map[msg.get('product_id')].insert_one(msg)

    def on_error(self, e):
        self.stop = True
        raise e


wsClient = myWSClient(url="wss://ws-feed.gdax.com", products=['BTC-USD', 'BCH-USD', 'ETH-USD', 'LTC-USD', 'ETH-BTC', 'LTC-BTC'], should_print=False)
wsClient.start()
while True:
    try:
        if wsClient.error:
            wsClient.close()
            print("Websocket Error... Restarting...")
            wsClient.error = None
            time.sleep(1)
            wsClient.start()
        time.sleep(1)
    except KeyboardInterrupt:
        wsClient.close()
        break
    except Exception:
        continue
