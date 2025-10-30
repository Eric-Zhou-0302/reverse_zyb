from module.market_data import MarketData
from module.exchange import Exchange
import datetime
import os
from tqdm import tqdm

'''
回测需要的参数：
'data_path': 数据路径
'start_time': 回测开始时间
'end_time': 回测结束时间
'interval': 回测时间间隔
'vwap_window': VWAP窗口
'estimate_window': 波动率估计窗口
'n_sigma': 阈值倍数
'initial_balance'：初始资金
'fee_rate': 手续费率
'''

def back_test(config):
    # 加载配置参数
    data_path = config.get('data_path', None)
    # 检查数据路径是否存在
    if data_path is None:
        raise ValueError("data_path is required")
    #开始时间
    start_date = config.get('start_date', None)
    #结束时间
    end_date = config.get('end_date', None)
    # 回测时间间隔
    interval = config.get('interval', 1)
    # VWAP窗口
    vwap_window = config.get('vwap_window', 20)
    # 波动率估计窗口
    estimate_window = config.get('estimate_window', 60*24)
    # 阈值倍数
    n_sigma = config.get('n_sigma', 3)
    # 初始资金
    initial_balance = config.get('initial_balance', 1000000)
    # 手续费率
    fee_rate = config.get('fee_rate', 0)

    # 日志
    # 检查是否存在日志目录，如果没有则生成
    if not os.path.exists('./Logging'):
        os.mkdir('./Logging')
    # 生成日志文件路径
    current_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = "Logging/" + current_timestamp + ".log"


    # 初始化市场数据和交易所
    market_data = MarketData(data_path, start_date, end_date, interval, vwap_window, estimate_window, n_sigma)
    exchange = Exchange(initial_balance=initial_balance, fee_rate=fee_rate, log_file=log_file)

    # 进度条数
    total_bars = market_data.get_total_bars()

    # 回测主循环
    with tqdm(total = total_bars, desc = '回测进度', unit = 'bar') as pbar:
        while market_data.has_more_data():
            current_bar = market_data.get_current_bar()
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
            market_data.next_bar()
            
            # 检查是否为最后一根K线，如果是且有持仓则强制平仓
            if not market_data.has_more_data():
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