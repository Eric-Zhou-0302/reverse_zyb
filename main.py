import json
from backtest_engine.single_backtest_engine import Single_Back_Test

config = json.load(open('base_config.json', 'r'))
Single_Back_Test(config)