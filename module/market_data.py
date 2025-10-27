import polars as pl


class MarketData:
    def __init__(self,config):
        self.data_path = config["data_path"]
        self.start_date = config.get("start_date", None)
        self.end_date = config.get("end_date", None)
        self.interval = config.get("interval", 1)  # 默认1分钟K线
        self.vwap_window = config.get("vwap_window", 20)  # VWAP计算窗口，默认20
        self.estimate_window = config.get("estimate_window", 60*24)  # 波动率估计窗口，默认60*24
        self.n_sigma = config.get("n_sigma", 3)  # 阈值倍数，默认2


        self.data = pl.read_parquet(self.data_path).sort(
            "open_time"
        ).with_columns(
            open_time = pl.col("open_time").cast(pl.Datetime(time_unit="ms"))
        )
        
        # 根据起始日期和结束日期过滤数据
        if self.start_date is not None and self.end_date is not None:
            self.data = self.data.filter(
                (pl.col("open_time") >= pl.lit(self.start_date).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")) &
                (pl.col("open_time") <= pl.lit(self.end_date).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"))
            )
        elif self.start_date is not None:
            self.data = self.data.filter(
                pl.col("open_time") >= pl.lit(self.start_date).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
            )
        elif self.end_date is not None:
            self.data = self.data.filter(
                pl.col("open_time") <= pl.lit(self.end_date).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")
            )
        
        # 过滤无效数据
        self.data = self.data.filter(pl.col("quote_volume") > 0)
        # k线聚合
        if self.interval > 1:
            self.data = self._aggregate_klines(self.data, self.interval)

        # 计算VWAP和阈值
        self.data = self.data.with_columns(
            vwap = pl.col("quote_volume").rolling_sum(self.vwap_window) / (pl.col("volume").rolling_sum(self.vwap_window) + 1)
        ).drop_nulls().with_columns(
            bias = (pl.col("close") / pl.col("vwap") - 1),
        ).with_columns(
            sigma = pl.col("bias").rolling_std(self.estimate_window),
        ).with_columns(
            bottom_threshold = pl.col("vwap") * (1 - self.n_sigma * pl.col("sigma")),
            top_threshold = pl.col("vwap") * (1 + self.n_sigma * pl.col("sigma"))
        ).drop_nulls()
        
        self.current_index = 0    
    def get_current_bar(self):
        """
        获取当前K线
        """
        if self.current_index < len(self.data):
            return self.data.row(self.current_index, named=True)
        else:
            return None
    
    def next_bar(self):
        """
        推进到下一根K线
        """
        self.current_index += 1
        return self.current_index < len(self.data)

    def has_more_data(self):
        return self.current_index < len(self.data)

    def get_total_bars(self):
        return len(self.data)
    
    def _aggregate_klines(self, data, interval):

        result = data.group_by_dynamic(
            "open_time",
            every=f"{interval}m",  # 每interval分钟分组
            closed="left", 
            label="left"  
        ).agg([
            # 开盘价：取第一个
            pl.col("open").first().alias("open"),
            # 最高价：取最大值
            pl.col("high").max().alias("high"),
            # 最低价：取最小值
            pl.col("low").min().alias("low"),
            # 收盘价：取最后一个
            pl.col("close").last().alias("close"),
            # 成交量：求和
            pl.col("volume").sum().alias("volume"),
            # 成交额：求和
            pl.col("quote_volume").sum().alias("quote_volume"),
            # 其他字段取第一个值
            pl.col("jj_code").first().alias("jj_code") if "jj_code" in data.columns else pl.lit(None).alias("jj_code")
        ]).sort("open_time")
        
        # 过滤掉没有数据的空时间段
        result = result.filter(pl.col("volume") > 0)
        
        return result