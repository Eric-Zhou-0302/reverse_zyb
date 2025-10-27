from tqdm import tqdm
import json
from module.market_data import MarketData
from module.exchange import Exchange
import datetime
import os

config = json.load(open("example_config.json", "r"))

# 获取起始日期、结束日期和interval
start_date = config.get('start_date', None)
end_date = config.get('end_date', None) 
interval = config.get('interval', None)

# 生成日志文件名
# 获取当前时间戳
current_timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
if '_log_file' in config:
    log_file = config['_log_file']
else:
    log_file = current_timestamp + '_' + os.path.basename(config['data_path']).replace('.parquet', 'trading.log')
# 初始化市场数据和交易所
data = MarketData(config)
exchange = Exchange(initial_balance=config.get("initial_balance", 10000), fee_rate=config.get("fee_rate", 0), log_file=log_file)

total_bars = data.get_total_bars()

last_minute = None

with tqdm(total=total_bars, desc="Processing Data", unit="bar") as pbar:
    while data.has_more_data():
        current_bar = data.get_current_bar()
        current_timestamp = current_bar['open_time']
        current_minute = current_timestamp.strftime('%Y-%m-%d %H:%M')
        current_price = current_bar['close']

        # 检查是否有未完成订单
        if exchange.limit_order:
            # 判断买卖点
            if current_bar['low'] <= exchange.limit_order.limit_price and exchange.limit_order.side == 'buy':
                fill_price = min(exchange.limit_order.limit_price, current_bar['open'])
                exchange.execute_limit_order(exchange.limit_order, fill_price, timestamp=current_timestamp)
            elif current_bar['high'] >= exchange.limit_order.limit_price and exchange.limit_order.side == 'sell':
                fill_price = max(exchange.limit_order.limit_price, current_bar['open'])
                exchange.execute_limit_order(exchange.limit_order, fill_price, timestamp=current_timestamp)

        # 基于持仓情况，每分钟均下单
        if exchange.position == 0:
            exchange.place_order('buy', current_bar["bottom_threshold"], timestamp=current_timestamp)
        elif exchange.position > 0:
            exchange.place_order('sell', current_bar["vwap"], timestamp=current_timestamp)

        # 记录每分钟净值
        exchange.record_minute_nav(current_minute, current_price)
        
        # 推进到下一根K线
        data.next_bar()
        
        # 检查是否为最后一根K线，如果是且有持仓则强制平仓
        if not data.has_more_data():
            # 这是最后一根K线，检查持仓状态
            if exchange.position > 0:
                # 使用当前K线的开盘价作为强制平仓价格
                force_close_price = current_bar['open']
                exchange.force_close_position(force_close_price, timestamp=current_timestamp)

        pbar.update(1)

#  保存交易记录表格为csv文件
exchange.save_trades_records().write_csv('trades_records.csv')

# # 计算回测指标
results = exchange.calculate_performance_metrics()

print("回测完成！")
print(f"总收益率（净值计算）: {results['total_returns']:.2%}")
print(f"总收益率（复利计算）: {results['compounded_total_returns']:.2%}")
print(f"简单年化收益率: {results['simple_annualized_returns']:.2%}")
print(f"复利年化收益率: {results['compounded_annualized_returns']:.2%}")
print(f"夏普比率: {results['sharpe_ratio']:.2f}")
print(f"最大回撤: {results['max_drawdown']:.2%}")
print(f"总交易对数: {results['num_trades']}")
print(f'胜率： {results["win_rate"]:.2%}')

# 关闭Exchange，确保日志系统正确关闭
exchange.close()