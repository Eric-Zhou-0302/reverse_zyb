from backtest_engine.single_backtest_engine import Single_Back_Test
import itertools
import pandas as pd
import datetime as dt

def configs_generator(base_config):
    """参数生成器（基于基本参数）"""
    vwap_windows = [20, 30, 40]
    estimate_windows = [60*24, 60*24*7, 60*24*30]
    n_sigmas = [1, 2, 3]

    configs = []
    for vwap_window, estimate_window, n_sigma in itertools.product(vwap_windows, estimate_windows, n_sigmas):
        config = base_config.copy()
        config['vwap_window'] = vwap_window
        config['estimate_window'] = estimate_window
        config['n_sigma'] = n_sigma
        configs.append(config)
    return configs

def batch_backtest_engine(configs):
    """批量回测引擎"""
    results = []
    success_count = 0
    for i, config in enumerate(configs):
        print(f"开始回测配置 {i+1}/{len(configs)}")
        try:
            result = Single_Back_Test(config)
            results.append(dict(**config, **result))
            print(f"配置{i+1}回测成功")
            success_count += 1
        except Exception as e:
            print(f"配置{i+1}回测失败：{e}")
    print(f'全部回测结束，共回测{len(configs)}个配置，成功{success_count}个，失败{len(configs)-success_count}个')
    save_results(results)
    print(f'结果保存在reverse_zyb/batch_backtest_results.csv文件中')

def save_results(results):
    """保存结果至csv"""
    # 获取当前时间戳
    timestamp = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
    # 生成Dataframe数组
    batch_backtest_results = pd.DataFrame(results)
    # 根据夏普比率降序排序
    batch_backtest_results = batch_backtest_results.sort_values(by='sharpe_ratio', ascending=False)
    batch_backtest_results.to_csv(f'batch_backtest_results_{timestamp}.csv', index=False)

if __name__ == '__main__':
    import json
    base_config = json.load(open('../base_config.json', 'r'))
    configs = configs_generator(base_config)
    batch_backtest_engine(configs)