import json
from single_backtest_engine import back_test

config = json.load(open('example_config.json', 'r'))
back_test(config)