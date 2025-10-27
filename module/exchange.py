import polars as pl
from datetime import datetime
from .buffered_logger import BufferedLogger
import numpy as np

# 订单类
class Order:
    def __init__(self, order_id, side, limit_price, order_type='limit'):
        self.order_id = order_id
        self.side = side  # "buy" or "sell"
        self.limit_price = limit_price
        self.quantity = None
        self.order_type = order_type
        self.fill_price = None

# 交易所类
class Exchange:
    def __init__(self, initial_balance=10000, fee_rate=0, log_file=None):
        # 基础账户信息
        self.initial_balance = initial_balance  # 初始资金
        self.cash = initial_balance     # 当前现金余额
        self.fee_rate = fee_rate    # 手续费率

        # 持仓信息
        self.position = 0   # 当前持仓：整数为多头，0为空仓
        self.position_cost = 0  # 持仓成本价

        # 订单管理
        self.order_id_counter = 0  # 订单ID计数器
        self.limit_order = None  # 当前订单

        # 交易记录
        self.trades = []    # 所有交易记录列表
        self.minute_nav = []     # 每分钟净值记录
        self.realized_pnl = 0   # 累计已实现盈亏
        self.trades_records = []   # 详细交易记录表格

        # 日志系统
        self.logger = BufferedLogger(log_file)  # 使用缓冲日志器记录交易过程

        # 时间管理
        self.current_timestamp = None   # 当前交易时间戳
    
    def place_order(self, side, limit_price=None, order_type='limit', timestamp=None):
        self.order_id_counter += 1
        self.limit_order = Order(self.order_id_counter, side, limit_price, order_type)
        # 记录下单日志，便于调试和分析
        time_str = timestamp.strftime('%Y-%m-%d %H:%M:%S') if timestamp else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.logger.info(f"{time_str} - 下单: {side.upper()} @ ${limit_price:.2f} ({order_type})")
    
    def execute_limit_order(self, order, fill_price, timestamp=None):
        """执行限价订单函数"""
        if order.side == "buy":
            # 买入操作
            order.quantity = self.cash / (fill_price * (1 + self.fee_rate))  # 计算可买入的最大数量
            fee = order.quantity * fill_price * self.fee_rate  # 计算手续费
            cost = order.quantity * fill_price + fee  # 计算总成本
            self.cash -= cost  # 扣除现金
            self.position += order.quantity  # 增加持仓
            self.position_cost = fill_price  # 更新持仓成本价
            self.logger.info(f"买入 {order.quantity} @ ${fill_price:.2f}, 成本 ${cost:.2f} (含手续费 ${fee:.2f})")
        elif order.side == "sell":
            # 卖出操作
            order.quantity = self.position  # 卖出所有持仓
            fee = order.quantity * fill_price * self.fee_rate  # 计算手续费
            revenue = order.quantity * fill_price - fee  # 计算总收入
            self.cash += revenue  # 增加现金
            self.realized_pnl += (fill_price - self.position_cost) * order.quantity - fee  # 更新已实现盈亏
            self.position = 0  # 清空持仓
            self.position_cost = 0  # 重置持仓成本价
            self.logger.info(f"卖出 {order.quantity} @ ${fill_price:.2f}, 收入 ${revenue:.2f} (含手续费 ${fee:.2f})")
        self.limit_order = None

        # 记录交易
        trade = {
            "timestamp": timestamp,
            "order_id": order.order_id,
            "side": order.side,
            "price": fill_price,
            "quantity": order.quantity,
            "fee": fee,
            "cash": self.cash,
            "position": self.position,
            "realized_pnl": self.realized_pnl
        }
        self.trades.append(trade)

    def get_portfolio_value(self, current_price):
        """计算当前组合价值 = 现金 + 仓位价值"""
        unrealized_pnl = current_price * self.position
        return (self.cash + unrealized_pnl)
    
    def record_minute_nav(self, date, current_price):
        """记录每分钟净值"""
        nav = self.get_portfolio_value(current_price)
        self.minute_nav.append({'date': date, 'nav': nav})

    def save_trades_records(self):
        self.trades_records = pl.DataFrame(self.trades)
        return pl.DataFrame(self.trades_records)

    def set_start_date(self, start_date):
        self.start_date = start_date

    def set_interval(self, interval):
        self.interval = interval
    
    def force_close_position(self, close_price, timestamp=None):
        """强制平仓函数 - 在交易结束时强制清空所有持仓"""
        if self.position > 0:
            # 取消当前未成交订单
            if self.limit_order:
                self.logger.info(f"取消未成交订单: {self.limit_order.side.upper()} @ ${self.limit_order.limit_price:.2f}")
                self.limit_order = None
            
            # 强制卖出所有持仓
            self.order_id_counter += 1
            quantity = self.position
            fee = quantity * close_price * self.fee_rate
            revenue = quantity * close_price - fee
            self.cash += revenue
            self.realized_pnl += (close_price - self.position_cost) * quantity - fee
            
            # 记录强制平仓日志
            time_str = timestamp.strftime('%Y-%m-%d %H:%M:%S') if timestamp else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger.info(f"{time_str} - 【强制平仓】卖出 {quantity:.6f} @ ${close_price:.2f}, 收入 ${revenue:.2f} (含手续费 ${fee:.2f}) - 交易结束强制清仓")
            
            # 记录交易记录
            trade = {
                "timestamp": timestamp,
                "order_id": self.order_id_counter,
                "side": "sell",
                "price": close_price,
                "quantity": quantity,
                "fee": fee,
                "cash": self.cash,
                "position": 0,
                "realized_pnl": self.realized_pnl
            }
            self.trades.append(trade)
            
            # 清空持仓
            self.position = 0
            self.position_cost = 0
            
            return True
        else:
            # 没有持仓，记录日志
            time_str = timestamp.strftime('%Y-%m-%d %H:%M:%S') if timestamp else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger.info(f"{time_str} - 【强制平仓检查】当前无持仓，无需强制平仓")
            return False

    def close(self):
        """关闭Exchange，确保日志系统正确关闭"""
        if hasattr(self, 'logger') and self.logger:
            self.logger.close()

    def calculate_performance_metrics(self):
        """计算回测指标"""
        # 检查是否有净值记录
        if not self.minute_nav:
            print("警告: 没有净值记录，无法计算回测指标")
            return {
                "total_returns": 0,
                "compounded_total_returns": 0,
                "simple_annualized_returns": 0,
                "compounded_annualized_returns": 0,
                "sharpe_ratio": 0
            }
            
        performance_metrics = pl.DataFrame(self.minute_nav)
        performance_metrics = performance_metrics.with_columns(
            returns = pl.col("nav").pct_change().fill_null(0)
        ).with_columns(
            cumulative_net_returns=(1 + pl.col("returns")).cum_prod(),
            cumulative_gross_returns=(1 + pl.col("returns")).cum_prod() - 1
        )
        returns = performance_metrics["returns"].to_numpy()
        # 计算总收益率（净值计算）
        total_returns = self.minute_nav[-1]["nav"] / self.minute_nav[0]["nav"] - 1
        # 计算总收益率（复利计算）
        compounded_total_returns = performance_metrics["cumulative_net_returns"].last() - 1
        # 计算年化收益率
        N_minutes = len(performance_metrics)
        # 简单年化
        simple_annualized_returns = returns.mean() * (365 * 24 * 60)
        # 复利年化
        compounded_annualized_returns = (1 + total_returns) ** (365 * 24 * 60 / N_minutes) - 1
        # 年化波动率
        annualized_volatility = returns.std() * np.sqrt(365 * 24 * 60)
        # 夏普比率
        sharpe_ratio = compounded_annualized_returns / annualized_volatility
        # 交易对数
        num_trades = len(self.trades) / 2 
        # 胜率
        wins = sum(1 for trade in self.trades if trade['side'] == 'sell' and (trade['realized_pnl'] > 0))
        win_rate = wins / num_trades if num_trades > 0 else 0
        # 最大回撤
        cumulative_returns = performance_metrics["cumulative_net_returns"].to_numpy()
        peak = np.maximum.accumulate(cumulative_returns)
        drawdowns = (peak - cumulative_returns) / peak
        max_drawdown = np.max(drawdowns)

        # 返回所有计算结果
        results = {
            "total_returns": total_returns,
            "compounded_total_returns": compounded_total_returns,
            "simple_annualized_returns": simple_annualized_returns,
            "compounded_annualized_returns": compounded_annualized_returns,
            "sharpe_ratio": sharpe_ratio,
            "num_trades": num_trades,
            "win_rate": win_rate,
            "max_drawdown": max_drawdown
        }
        return results