import json
import sys
from backtest_engine.single_backtest_engine import Single_Back_Test
from backtest_engine.batch_backtest_engine import batch_backtest_engine, configs_generator


if len(sys.argv) < 2:
    print('请输入参数：single 或 batch')
    exit()
if sys.argv[1] == 'single':
    # 执行单次回测
    config = json.load(open('base_config.json', 'r'))
    Single_Back_Test(config)
if sys.argv[1] == 'batch':
    # 执行批量回测
    base_config = json.load(open('base_config.json', 'r'))
    configs = configs_generator(base_config)
    batch_backtest_engine(configs)

