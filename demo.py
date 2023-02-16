import pandas as pd


def load_daily():
    """
    加载日线数据
    """
    data = []
    with open('E:\\股票数据\\沪深A股\\SH#600062.txt', encoding='gbk') as f:
        for idx, line in enumerate(f):
            if idx <= 1:
                continue
            if line.strip() == "数据来源:通达信":
                break
            splits = line.strip().split('\t')
            # 2000/01/04	-1.54	-1.45	-1.57	-1.47	4496000	113946784.00
            row = [
                splits[0],
                float(splits[1]),
                float(splits[2]),
                float(splits[3]),
                float(splits[4]),
                int(splits[5]),
                float(splits[6])
            ]
            data.append(row)

    df = pd.DataFrame(data, columns=['date', 'open', 'high', 'low', 'close', 'volume', 'money'])
    return df

df = load_daily()

# 将交易日期字符串变为日期类型
df['date'] = pd.to_datetime(df['date'])  
# 将日期列设为索引
df.set_index('date', inplace = True)
df.sort_index(inplace = True)

print(df.tail(20))


# 定义一个空的df
week_df = pd.DataFrame()

# 求每周的开盘价
week_df['open'] = df['open'].resample('W').first()
# 求每周的收盘价
week_df['close'] = df['close'].resample('W').last()
# 求每周的最大值
week_df['high'] = df['high'].resample('W').max()
# 求每周的最小值
week_df['low'] = df['low'].resample('W').min()

week_df.to_csv('E:\\股票数据\\WEEK#SH#600062.txt', sep="\t")

# print(week_df.tail(10))
# print(week_df.loc[['2022-01-02']])

