import os
import sys
import json
import datetime as dt
import polars as pl
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from module.market_data import MarketData
from module.exchange import Exchange


def run_backtest_collect_data(config: dict):
    """运行回测并收集绘图与报告数据"""
    data_path = config.get("data_path", None)
    if data_path is None:
        raise ValueError("data_path is required")

    start_date = config.get("start_date", None)
    end_date = config.get("end_date", None)
    interval = config.get("interval", 1)
    vwap_window = config.get("vwap_window", 20)
    estimate_window = config.get("estimate_window", 60 * 24)
    n_sigma = config.get("n_sigma", 3)
    initial_balance = config.get("initial_balance", 1000000)
    fee_rate = config.get("fee_rate", 0)

    market_data = MarketData(
        data_path,
        start_date,
        end_date,
        interval,
        vwap_window,
        estimate_window,
        n_sigma,
    )
    current_timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    if not os.path.exists("./Logging"):
        os.mkdir("./Logging")
    exchange = Exchange(initial_balance=initial_balance, fee_rate=fee_rate, log_file=f"Logging/{current_timestamp}.log")

    total_bars = market_data.get_total_bars()
    while market_data.has_more_data():
        current_bar = market_data.get_current_bar()
        ts = current_bar["open_time"]

        if exchange.limit_order:
            if current_bar["low"] <= exchange.limit_order.limit_price and exchange.limit_order.side == "buy":
                fill_price = min(exchange.limit_order.limit_price, current_bar["open"])
                exchange.execute_limit_order(exchange.limit_order, fill_price, timestamp=ts)
            elif current_bar["high"] >= exchange.limit_order.limit_price and exchange.limit_order.side == "sell":
                fill_price = max(exchange.limit_order.limit_price, current_bar["open"])
                exchange.execute_limit_order(exchange.limit_order, fill_price, timestamp=ts)

        if exchange.position == 0:
            exchange.place_order("buy", current_bar["bottom_threshold"], timestamp=ts)
        else:
            exchange.place_order("sell", current_bar["vwap"], timestamp=ts)

        price = current_bar["close"]
        exchange.record_minute_nav(ts.strftime("%Y-%m-%d %H:%M"), price)

        market_data.next_bar()

        if not market_data.has_more_data():
            if exchange.position > 0:
                force_close_price = current_bar["open"]
                exchange.force_close_position(force_close_price, timestamp=ts)

    results = exchange.calculate_performance_metrics()
    trades_df = pl.DataFrame(exchange.trades) if exchange.trades else pl.DataFrame({})
    minute_nav_df = pl.DataFrame(exchange.minute_nav) if exchange.minute_nav else pl.DataFrame({})
    price_df = market_data.data.select(["open_time", "close", "vwap", "bottom_threshold", "top_threshold"]).with_columns(
        open_time_str=pl.col("open_time").dt.strftime("%Y-%m-%d %H:%M")
    )

    return results, trades_df, minute_nav_df, price_df


def compute_trade_pnl_df(trades_df: pl.DataFrame) -> pl.DataFrame:
    """计算每笔卖出交易的单笔盈亏"""
    if trades_df.is_empty():
        return pl.DataFrame({})
    sells = trades_df.filter(pl.col("side") == "sell")
    if sells.is_empty():
        return pl.DataFrame({})
    if "pnl" in sells.columns:
        return sells.select(["timestamp", "price", "quantity", "fee", "pnl"]).sort("pnl", descending=True)
    sells = sells.with_columns(prev_cum=pl.col("realized_pnl").shift(1).fill_null(0))
    sells = sells.with_columns(pnl=pl.col("realized_pnl") - pl.col("prev_cum"))
    return sells.select(["timestamp", "price", "quantity", "fee", "pnl"]).sort("pnl", descending=True)


def build_html(config: dict, results: dict, trades_df: pl.DataFrame, trade_pnl_df: pl.DataFrame, minute_nav_df: pl.DataFrame, price_df: pl.DataFrame) -> str:
    """构建回测报告HTML页面"""
    import json as pyjson

    config_json = pyjson.dumps(config, ensure_ascii=False, indent=2)
    
    # Format results items for display
    results_items = "".join([
        f"<div class='metric-card'><h3>{k}</h3><p>{v}</p></div>" for k, v in results.items()
    ])

    top_profit = trade_pnl_df.head(10) if not trade_pnl_df.is_empty() else pl.DataFrame({})
    top_loss = trade_pnl_df.sort("pnl").head(10) if not trade_pnl_df.is_empty() else pl.DataFrame({})

    def table_html(df: pl.DataFrame) -> str:
        if df.is_empty():
            return "<div class='no-data'>无交易数据</div>"
        cols = df.columns
        header = "".join([f"<th>{c.replace('_', ' ').title()}</th>" for c in cols])
        rows = "".join([
            "<tr>" + "".join([f"<td>{row[i]}</td>" for i in range(len(cols))]) + "</tr>"
            for row in df.iter_rows()
        ])
        return f"<div class='table-container'><table><thead><tr>{header}</tr></thead><tbody>{rows}</tbody></table></div>"

    nav_dates = minute_nav_df["date"].to_list() if "date" in minute_nav_df.columns else []
    nav_values = minute_nav_df["nav"].to_list() if "nav" in minute_nav_df.columns else []

    drawdown_df = minute_nav_df
    if not minute_nav_df.is_empty():
        drawdown_df = drawdown_df.with_columns(peak=pl.col("nav").cum_max())
        drawdown_df = drawdown_df.with_columns(drawdown=(pl.col("peak") - pl.col("nav")) / pl.col("peak"))
    dd_values = drawdown_df["drawdown"].to_list() if "drawdown" in drawdown_df.columns else []

    price_time = price_df["open_time_str"].to_list() if "open_time_str" in price_df.columns else []
    price_close = price_df["close"].to_list() if "close" in price_df.columns else []
    price_vwap = price_df["vwap"].to_list() if "vwap" in price_df.columns else []
    price_bottom = price_df["bottom_threshold"].to_list() if "bottom_threshold" in price_df.columns else []
    price_top = price_df["top_threshold"].to_list() if "top_threshold" in price_df.columns else []

    buys = trades_df.filter(pl.col("side") == "buy") if not trades_df.is_empty() else pl.DataFrame({})
    sells = trades_df.filter(pl.col("side") == "sell") if not trades_df.is_empty() else pl.DataFrame({})
    buy_times = buys["timestamp"].dt.strftime("%Y-%m-%d %H:%M").to_list() if not buys.is_empty() else []
    buy_prices = buys["price"].to_list() if not buys.is_empty() else []
    sell_times = sells["timestamp"].dt.strftime("%Y-%m-%d %H:%M").to_list() if not sells.is_empty() else []
    sell_prices = sells["price"].to_list() if not sells.is_empty() else []

    html = f"""
<!DOCTYPE html>
<html lang='zh-CN'>
<head>
  <meta charset='UTF-8' />
  <meta name='viewport' content='width=device-width, initial-scale=1.0' />
  <title>回测报告 - Quant Report</title>
  <script src='https://cdn.plot.ly/plotly-2.26.0.min.js'></script>
  <style>
    :root {{
      --bg-color: #f8f9fa;
      --card-bg: #ffffff;
      --text-primary: #2d3748;
      --text-secondary: #718096;
      --accent-color: #3182ce;
      --border-color: #e2e8f0;
      --success-color: #38a169;
      --danger-color: #e53e3e;
    }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
      margin: 0;
      padding: 24px;
      background-color: var(--bg-color);
      color: var(--text-primary);
    }}
    .container {{
      max_width: 1200px;
      margin: 0 auto;
    }}
    h1, h2, h3 {{ color: var(--text-primary); margin-top: 0; }}
    h1 {{ font-size: 2rem; margin-bottom: 24px; border-bottom: 2px solid var(--accent-color); padding-bottom: 12px; display: inline-block; }}
    h2 {{ font-size: 1.5rem; margin: 32px 0 16px; color: var(--text-secondary); }}
    
    .card {{
      background: var(--card-bg);
      border-radius: 8px;
      box-shadow: 0 4px 6px rgba(0, 0, 0, 0.05);
      padding: 24px;
      margin-bottom: 24px;
    }}
    
    .metrics-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }}
    .metric-card {{
      background: var(--card-bg);
      border: 1px solid var(--border-color);
      border-radius: 6px;
      padding: 16px;
      text-align: center;
    }}
    .metric-card h3 {{ font-size: 0.9rem; color: var(--text-secondary); margin-bottom: 8px; font-weight: normal; }}
    .metric-card p {{ font-size: 1.25rem; font-weight: bold; margin: 0; color: var(--text-primary); }}
    
    pre {{ background: #edf2f7; padding: 16px; border-radius: 6px; overflow: auto; font-size: 0.85rem; border: 1px solid var(--border-color); }}
    
    .table-container {{ overflow-x: auto; border: 1px solid var(--border-color); border-radius: 6px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.9rem; }}
    th, td {{ padding: 12px 16px; text-align: left; border-bottom: 1px solid var(--border-color); }}
    th {{ background-color: #f7fafc; font-weight: 600; color: var(--text-secondary); white-space: nowrap; }}
    tr:last-child td {{ border-bottom: none; }}
    tr:hover {{ background-color: #f7fafc; }}
    
    .chart-container {{ height: 450px; width: 100%; }}
    .chart-sm {{ height: 350px; }}
    
    .no-data {{ padding: 24px; text-align: center; color: var(--text-secondary); font-style: italic; }}
    
    .grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }}
    @media (max-width: 768px) {{ .grid-2 {{ grid-template-columns: 1fr; }} }}
  </style>
</head>
<body>
  <div class='container'>
    <h1>回测报告</h1>
    
    <div class='card'>
      <h2>核心指标</h2>
      <div class='metrics-grid'>
        {results_items}
      </div>
    </div>

    <div class='grid-2'>
      <div class='card'>
        <h2>净值曲线</h2>
        <div id='nav_chart' class='chart-sm'></div>
      </div>
      <div class='card'>
        <h2>最大回撤</h2>
        <div id='dd_chart' class='chart-sm'></div>
      </div>
    </div>

    <div class='card'>
      <h2>价格走势与交易点位</h2>
      <div id='price_chart' class='chart-container'></div>
    </div>

    <div class='grid-2'>
      <div class='card'>
        <h2>前十大盈利交易</h2>
        {table_html(top_profit)}
      </div>
      <div class='card'>
        <h2>前十大亏损交易</h2>
        {table_html(top_loss)}
      </div>
    </div>

    <div class='card'>
      <h2>回测配置</h2>
      <pre>{config_json}</pre>
    </div>
  </div>

  <script>
    const navDates = {pyjson.dumps(nav_dates)};
    const navValues = {pyjson.dumps(nav_values)};
    const ddValues = {pyjson.dumps(dd_values)};

    const priceTime = {pyjson.dumps(price_time)};
    const priceClose = {pyjson.dumps(price_close)};
    const priceVwap = {pyjson.dumps(price_vwap)};
    const priceBottom = {pyjson.dumps(price_bottom)};
    const priceTop = {pyjson.dumps(price_top)};

    const buyTimes = {pyjson.dumps(buy_times)};
    const buyPrices = {pyjson.dumps(buy_prices)};
    const sellTimes = {pyjson.dumps(sell_times)};
    const sellPrices = {pyjson.dumps(sell_prices)};

    const layoutDefaults = {{
      font: {{ family: "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif" }},
      margin: {{ t: 30, l: 50, r: 20, b: 40 }},
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      xaxis: {{ showgrid: false, zeroline: false }},
      yaxis: {{ showgrid: true, gridcolor: '#e2e8f0', zeroline: false }},
      showlegend: true,
      legend: {{ orientation: 'h', y: 1.1 }}
    }};

    Plotly.newPlot('nav_chart', [
      {{ x: navDates, y: navValues, type: 'scatter', mode: 'lines', name: 'NAV', line: {{ color: '#3182ce', width: 2 }}, fill: 'tozeroy', fillcolor: 'rgba(49, 130, 206, 0.1)' }}
    ], {{ ...layoutDefaults, yaxis: {{ ...layoutDefaults.yaxis, tickformat: ',.2f' }} }});

    Plotly.newPlot('dd_chart', [
      {{ x: navDates, y: ddValues, type: 'scatter', mode: 'lines', name: 'Drawdown', line: {{ color: '#e53e3e', width: 2 }}, fill: 'tozeroy', fillcolor: 'rgba(229, 62, 62, 0.1)' }}
    ], {{ ...layoutDefaults, yaxis: {{ ...layoutDefaults.yaxis, tickformat: '.2%' }} }});

    const traces = [
      {{ x: priceTime, y: priceClose, type: 'scatter', mode: 'lines', name: 'Price', line: {{ color: '#4a5568', width: 1.5 }} }},
      {{ x: priceTime, y: priceVwap, type: 'scatter', mode: 'lines', name: 'VWAP', line: {{ color: '#ed8936', width: 1.5 }} }},
      {{ x: priceTime, y: priceBottom, type: 'scatter', mode: 'lines', name: 'Bottom', line: {{ color: '#48bb78', dash: 'dot', width: 1 }} }},
      {{ x: priceTime, y: priceTop, type: 'scatter', mode: 'lines', name: 'Top', line: {{ color: '#805ad5', dash: 'dot', width: 1 }} }},
      {{ x: buyTimes, y: buyPrices, type: 'scatter', mode: 'markers', name: 'Buy Signal', marker: {{ color: '#38a169', size: 8, symbol: 'triangle-up', line: {{ color: '#fff', width: 1 }} }} }},
      {{ x: sellTimes, y: sellPrices, type: 'scatter', mode: 'markers', name: 'Sell Signal', marker: {{ color: '#e53e3e', size: 8, symbol: 'triangle-down', line: {{ color: '#fff', width: 1 }} }} }}
    ];
    Plotly.newPlot('price_chart', traces, layoutDefaults);
  </script>
</body>
</html>
"""
    return html


def generate_backtest_report(config: dict, output_html_path: str | None = None) -> str:
    """生成回测结果HTML报告并返回文件路径"""
    results, trades_df, minute_nav_df, price_df = run_backtest_collect_data(config)
    trade_pnl_df = compute_trade_pnl_df(trades_df)
    html = build_html(config, results, trades_df, trade_pnl_df, minute_nav_df, price_df)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    if not os.path.exists("./Results"):
        os.mkdir("./Results")
    output = output_html_path or f"Results/backtest_report_{ts}.html"
    with open(output, "w", encoding="utf-8") as f:
        f.write(html)
    return output


def generate_report_from_run(config: dict, exchange: Exchange, market_data: MarketData, output_html_path: str | None = None) -> str:
    """基于已完成的回测数据生成HTML报告并返回文件路径"""
    results = exchange.calculate_performance_metrics()
    trades_df = pl.DataFrame(exchange.trades) if exchange.trades else pl.DataFrame({})
    minute_nav_df = pl.DataFrame(exchange.minute_nav) if exchange.minute_nav else pl.DataFrame({})
    price_df = market_data.data.select(["open_time", "close", "vwap", "bottom_threshold", "top_threshold"]).with_columns(
        open_time_str=pl.col("open_time").dt.strftime("%Y-%m-%d %H:%M")
    )
    trade_pnl_df = compute_trade_pnl_df(trades_df)
    html = build_html(config, results, trades_df, trade_pnl_df, minute_nav_df, price_df)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    if not os.path.exists("./Results"):
        os.mkdir("./Results")
    output = output_html_path or f"Results/backtest_report_{ts}.html"
    with open(output, "w", encoding="utf-8") as f:
        f.write(html)
    return output


if __name__ == "__main__":
    """命令行入口：读取base_config并生成报告"""
    cfg_path = os.path.join(os.path.dirname(__file__), "..", "base_config.json")
    cfg = json.load(open(cfg_path, "r"))
    out = generate_backtest_report(cfg)
    print(f"报告已生成: {out}")
