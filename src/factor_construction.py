import os
import polars as pl
from tqdm import tqdm


data_daily_path = "data/data_daily"
factor_path = "data/factor"


columns = ["date", "code", "open", "close", "low", "high", "volume", "money", "turnover_ratio"]

# 创建因子文件夹的函数
def create_factor_folder(factor_name):
    factor_folder = os.path.join(factor_path, factor_name)
    os.makedirs(factor_folder, exist_ok=True)
    return factor_folder

# 定义处理所有CSV文件的函数
def process_all_stock_data():
    all_data = []
    for filename in tqdm(os.listdir(data_daily_path)):
        if filename.endswith(".csv"):
            full_path = os.path.join(data_daily_path, filename)
            df = pl.read_csv(full_path, new_columns=columns)
            if len(df) > 0:
                all_data.append(df)
    all_data = pl.concat(all_data)            
    all_data = all_data.with_columns(
            pl.col("date").str.strptime(pl.Date, "%Y-%m-%d").alias("date")
        )
    all_data = all_data.sort(["code", "date"])
    return all_data

# 因子1: 动量因子
def calculate_momentum_factor(df, window=5):

    df = df.group_by("code").agg([
        pl.col("date"),
        (pl.col("close") / pl.col("close").shift(window) - 1).alias("momentum_5d")
    ])
    return df.explode(["date", "momentum_5d"])

# 因子2: 价格振幅因子
def calculate_price_amplitude_factor(df, window=None):  # 添加默认参数

    df = df.group_by("code").agg([
        pl.col("date"),
        (pl.col("high") - pl.col("low")).alias("price_amplitude")
    ])
    return df.explode(["date", "price_amplitude"])

# 因子3: 开盘价与收盘价差值因子
def calculate_open_close_diff_factor(df, window=None):  

    df = df.group_by("code").agg([
        pl.col("date"),
        (pl.col("open") - pl.col("close")).alias("open_close_diff")
    ])
    return df.explode(["date", "open_close_diff"])

# 因子4: 最高价与收盘价差值因子
def calculate_high_close_diff_factor(df, window=None):  

    df = df.group_by("code").agg([
        pl.col("date"),
        (pl.col("high") - pl.col("close")).alias("high_close_diff")
    ])
    return df.explode(["date", "high_close_diff"])

# 因子5: 最低价与收盘价差值因子
def calculate_low_close_diff_factor(df, window=None):  

    df = df.group_by("code").agg([
        pl.col("date"),
        (pl.col("low") - pl.col("close")).alias("low_close_diff")
    ])
    return df.explode(["date", "low_close_diff"])

# 因子6: 成交量与换手率比值因子
def calculate_volume_turnover_ratio_factor(df, window=None): 

    df = df.group_by("code").agg([
        pl.col("date"),
        (pl.col("volume") / pl.col("turnover_ratio")).alias("volume_turnover_ratio")
    ])
    return df.explode(["date", "volume_turnover_ratio"])

# 保存到相应文件夹
def process_and_save_factors(factor_name, process_func, window=5):
    factor_folder = create_factor_folder(factor_name)
    all_data = process_all_stock_data()
    all_data = process_func(all_data, window)

    # 按日期分组，保存到相应的CSV文件中
    for date_tuple, group in all_data.group_by("date"):
        date_string = date_tuple[0]  # date_tuple 是一个元组，取第一个元素
        group.write_csv(os.path.join(factor_folder, f"{date_string}.csv"))

# 主函数
def main():
    process_and_save_factors("momentum_5d", calculate_momentum_factor, window=5)

    process_and_save_factors("price_amplitude", calculate_price_amplitude_factor)

    process_and_save_factors("open_close_diff", calculate_open_close_diff_factor)

    process_and_save_factors("high_close_diff", calculate_high_close_diff_factor)

    process_and_save_factors("low_close_diff", calculate_low_close_diff_factor)

    process_and_save_factors("volume_turnover_ratio", calculate_volume_turnover_ratio_factor)

if __name__ == "__main__":
    main()