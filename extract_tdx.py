import os
import sys
import time
import pandas as pd
from struct import unpack
from datetime import datetime

# 获取当前目录
proj_path = os.path.dirname(os.path.abspath(sys.argv[0]))


# 将通达信的日线文件转换成CSV格式
def day2csv(source_fname, output_fname):
    # 以二进制方式打开源文件
    f = open(source_fname, "rb")
    buf = f.read()
    f.close()

    # 打开目标文件，后缀名为CSV
    target_file = open(output_fname, 'w')
    buf_size = len(buf)
    rec_count = int(buf_size / 32)
    begin = 0
    end = 32
    header = str('date') + ',' + str('open') + ',' + str('high') + ',' + str('low') + ',' \
             + str('close') + ',' + str('amount') + ',' + str('volume') + '\n'
    target_file.write(header)
    for i in range(rec_count):
        # 将字节流转换成Python数据格式
        # I: unsigned int
        # f: float
        a = unpack('IIIIIfII', buf[begin:end])
        # 处理date数据
        year = a[0] // 10000
        month = (a[0] % 10000) // 100
        day = (a[0] % 10000) % 100
        date = '{}-{:02d}-{:02d}'.format(year, month, day)

        line = date + ',' + str(a[1] / 100.0) + ',' + str(a[2] / 100.0) + ',' \
               + str(a[3] / 100.0) + ',' + str(a[4] / 100.0) + ',' + str(a[5]) + ',' \
               + str(a[6]) + '\n'
        target_file.write(line)
        begin += 32
        end += 32
    target_file.close()


def get_market_from_code(stock_code):
    return stock_code[:2]


def transform_data(stock_code):
    market = get_market_from_code(stock_code)
    # 保存csv文件的目录
    output_dir = proj_path + '/data/tdx/day'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    code_list = []
    source_dir = 'D:/Program Files (x86)/new_tdx/vipdoc/%s/lday' % market
    file_list = os.listdir(source_dir)
    target_fname = "%s.day" % stock_code
    # 逐个文件进行解析
    for fname in file_list:
        if fname != target_fname:
            continue
        source_fname = os.path.join(source_dir, fname)
        output_fname = os.path.join(output_dir, fname.replace(".day", ".csv"))
        day2csv(source_fname, output_fname)
    
    # 获取所有股票/指数代码
    # code_list.extend(list(map(lambda x: x[:x.rindex('.')], file_list)))
    # 保存所有代码列表
    # pd.DataFrame(data=code_list, columns=['code']).to_csv(proj_path + '/data/tdx/all_codes.csv', index=False)


def extract_data(from_date, file_name):
    # 以二进制方式打开源文件
    source_file = open(file_name, 'rb')
    buf = source_file.read()
    source_file.close()
    buf_size = len(buf)
    rec_count = int(buf_size / 32)
    # 从文件末开始访问数据
    begin = buf_size - 32
    end = buf_size
    data = []
    for i in range(rec_count):
        # 将字节流转换成Python数据格式
        # I: unsigned int
        # f: float
        a = unpack('IIIIIfII', buf[begin:end])
        # 处理date数据
        year = a[0] // 10000
        month = (a[0] % 10000) // 100
        day = (a[0] % 10000) % 100
        date = '{}-{:02d}-{:02d}'.format(year, month, day)
        if from_date == date:
            break
        data.append([date, str(a[1] / 100.0), str(a[2] / 100.0), str(a[3] / 100.0), \
                     str(a[4] / 100.0), str(a[5]), str(a[6])])
        begin -= 32
        end -= 32
    # 反转数据
    data.reverse()
    return data


def update_data():
    # 读入所有股票/指数代码
    codes = pd.read_csv(proj_path + 'data/tdx/all_codes.csv')['code']
    for code in codes:
        data_path = proj_path + 'data/tdx/day/' + code + '.csv'
        # 读取当前已存在的数据
        exist_df = pd.read_csv(data_path)
        # 获取需要更新的日线开始时间
        from_date = pd.read_csv(proj_path + 'data/tdx/day/' + code + '.csv')['date'].iloc[-1]
        # 提取新数据
        data = extract_data(from_date, 'C:/new_tdx/vipdoc/' + code[0:2] + '/lday/' + code + '.day')
        if not len(data):
            continue
        df = pd.DataFrame(data).rename(
            columns={0: 'date', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'amount', 6: 'volume'})
        # 合并数据
        df = exist_df.append(df)
        # 保存文件
        df.to_csv(data_path, index=False)


def get_all_stock_codes():
    all_codes_file = proj_path + 'data/tdx/all_codes.csv'
    if not os.path.exists(all_codes_file):
        print('请先更新数据！')
        return
    df = pd.read_csv(all_codes_file)
    df = df[((df['code'] >= 'sh600000') & (df['code'] <= 'sh605999')) | \
            ((df['code'] >= 'sz000001') & (df['code'] <= 'sz003999')) | \
            ((df['code'] >= 'sz300000') & (df['code'] <= 'sz300999'))]
    df.to_csv(proj_path + 'data/tdx/all_stock_codes.csv', index=False)



def day2week(stock_code):
    """
    日线转周线
    输出：
    date(周五),open,high,low,close,amount,volume,increase
    计算过程：
        初始化变量字典：{open,high,low,close,amount,volume}
        初始化上一周交易收盘价，如果是第1周，则初始化为None；
        从第一天开始遍历, 直到遇到周五结束：
            如果是第1天，更新open数值
            如果是周五，更新close数值
            high = max(high, day_high)
            low = min(low, day_low)
            amount = amount + day_amount
            volume = volume + day_volume
        开始计算以下指标：
            涨跌：（收盘价 - 上周最后一个交易日的收盘价）  【如果上周最后一个交易日的收盘价为空，则不计算，设置为控制】
            涨幅：（收盘价 - 上周最后一个交易日的收盘价） / 上周最后一个交易日的收盘价
            振幅：（最高价－最低价）/ 上周最后一个交易日的收盘价
            # 换手率：交易量 / 流通股数 (流通股数在变化，暂时不计算)
    """
    def reset_weekdict(week_dict):
        week_dict["open"] = 0.0
        week_dict["high"] = 0.0
        week_dict["low"] = 100000000.0
        week_dict["close"] = 0.0
        week_dict["amount"] = 0
        week_dict["volume"] = 0.0
        week_dict["increase"] = None
        week_dict["increase_r"] = None
        week_dict["strike_r"] = None
        week_dict["lowest_weeks"] = -1     # 当周收盘价是最近多少周的最低收盘价
        week_dict["special"] = 0     # 10周内，出现过的最大lowest_weeks
    
    df = pd.read_csv("./data/tdx/day/%s.csv" % stock_code)
    
    last_close = None
    is_first_day = True
    
    week_dict = dict()
    reset_weekdict(week_dict)
    
    output_cols = ["date","open","high","low","close","amount","volume","increase","increase_r","strike_r", "lowest_weeks", "special"]
    lowest_price_list = []
    lowest_weeks_list = []
    wr = open("./data/tdx/week/%s.csv" % stock_code, "w")
    wr.write(",".join(output_cols) + "\n")
    for idx, row in df.iterrows():
        row_dict = dict(row)
        # print(row_dict)
        
        if is_first_day:
            week_dict["open"] = row_dict["open"]
            is_first_day = False
        
        week_dict["high"] = max(week_dict["high"], row_dict["high"])
        week_dict["low"] = min(week_dict["low"], row_dict["low"])
        week_dict["amount"] += row_dict["amount"]
        week_dict["volume"] += row_dict["volume"]
        
        job_date = datetime.strptime(row_dict["date"], '%Y-%m-%d')
        week_day = job_date.weekday()
        
        # 周五
        if week_day == 4:
            week_dict["date"] = row_dict["date"]
            week_dict["close"] = row_dict["close"]
            
            if last_close is not None:
                week_dict["increase"] = round((week_dict["close"] - last_close), 4)
                week_dict["increase_r"] = round(week_dict["increase"] / last_close, 4)
                week_dict["strike_r"] = round((week_dict["high"] - week_dict["low"]) / last_close, 4)
            
            # 计算lowest_weeks
            for i in range(1, len(lowest_price_list) + 1):
                j = -1 * i
                if week_dict["low"] > lowest_price_list[j]:
                    week_dict["lowest_weeks"] = i - 1
                    break
            # 如果没有找到更低的，说明这是历史最低价
            if week_dict["lowest_weeks"] == -1:
                week_dict["lowest_weeks"] = len(lowest_price_list)
            
            # 计算special
            for i in range(1, min(11, len(lowest_weeks_list)+1)):
                week_dict["special"] = max(week_dict["special"], lowest_weeks_list[-i])

            output_lst = [str(week_dict[k]) if week_dict[k] is not None else "" for k in output_cols]
            #print("%s,%s,%s,%s,%s,%s,%s,%s" % (x for k in ["date","open","high","low","close","amount","volume",increase]))
            #print(",".join(output_lst))
            wr.write(",".join(output_lst) + "\n")
            
            is_first_day = True
            last_close = week_dict["close"]
            lowest_price_list.append(week_dict["low"])
            lowest_weeks_list.append(week_dict["lowest_weeks"])
            reset_weekdict(week_dict)
        
        # if row_dict["date"] == "2017-07-28":
        #     break
    
    wr.close()


# def main():
#     # 程序开始时的时间
#     time_start = time.time()

#     # 获取所有股票代码
#     # get_all_stock_codes()

#     # 转换所有数据
#     # transform_data()

#     # 更新数据
#     # update_data()

#     # 程序结束时系统时间
#     time_end = time.time()

#     print('程序所耗时间：', time_end - time_start)

def test():
    week_dict = {}
    week_dict["lowest_days"] = -1
    week_dict["low"] = 2.19
    lowest_price_list = [20.4, 20.52, 19.5, 19.2, 20.48, 19.98, 20.1, 20.1, 20.1, 21.29, 21.56, 22.14, 22.53, 22.0, 22.01, 20.5, 19.95, 20.65, 20.3, 20.18, 20.08, 19.21, 19.18, 18.3, 17.5, 17.79, 18.0, 18.58, 17.81, 18.45, 17.61, 18.05, 16.7, 16.35, 15.45, 15.09, 15.25, 15.12, 16.0, 16.05, 16.4, 16.9, 16.16, 16.18, 16.52, 16.31, 16.61, 17.64, 18.8, 18.6, 18.2, 17.51, 17.62, 17.85, 17.79, 17.8, 17.53, 17.0, 17.4, 17.3, 17.5, 17.73, 17.3, 17.5, 17.8, 18.0, 18.2, 18.47, 18.02, 18.17, 17.68, 17.28, 17.01, 16.4, 15.25, 14.1, 15.3, 15.0, 14.78, 14.8, 14.8, 14.51, 13.8, 12.03, 11.5, 11.0, 12.04, 11.23, 11.5, 11.83, 12.4, 13.6, 
12.82, 12.41, 12.12, 12.38, 11.4, 9.5, 9.0, 8.43, 9.85, 10.04, 10.08, 11.01, 11.1, 11.58, 10.75, 11.3, 11.16, 11.12, 11.69, 10.75, 10.76, 11.15, 10.9, 11.6, 11.2, 12.51, 12.32, 12.5, 12.45, 12.07, 11.91, 11.4, 10.92, 11.0, 11.08, 10.8, 10.7, 10.48, 10.48, 10.54, 10.9, 10.98, 11.05, 10.99, 9.28, 8.65, 
8.79, 8.9, 8.91, 9.21, 8.78, 8.2, 8.28, 8.56, 9.0, 9.04, 9.28, 9.3, 9.11, 8.75, 8.65, 8.76, 7.92, 7.88, 7.66, 6.71, 7.3, 7.42, 7.59, 7.3, 7.25, 7.05, 7.16, 7.17, 7.05, 6.92, 6.72, 6.72, 6.75, 6.69, 6.71, 6.73, 6.32, 6.24, 6.11, 6.18, 5.75, 4.98, 4.52, 4.6, 4.81, 5.1, 5.36, 5.59, 4.98, 4.98, 5.05, 5.85, 6.26, 6.42, 6.65, 6.9, 7.4, 7.15, 7.35, 6.9, 7.05, 7.48, 7.19, 7.65, 6.8, 6.5, 6.53, 6.08, 6.4, 6.0, 6.2, 6.06, 5.6, 5.47, 5.26, 5.29, 5.16, 4.94, 4.8, 4.8, 4.64, 4.4, 4.34, 4.33, 4.4, 4.29, 4.73, 4.4, 4.25, 3.8, 3.63, 3.71, 4.0, 4.14, 4.33, 4.68, 4.54, 4.2, 4.22, 4.34, 4.24, 4.25, 4.11, 3.86, 3.67, 3.98, 3.9, 4.15, 4.24, 3.94, 3.72, 3.5, 3.63, 3.65, 3.26, 3.14, 2.95, 3.01, 3.05, 3.15, 3.16, 3.37, 3.35, 3.26, 3.1, 3.0, 2.93, 2.98, 3.15, 3.4, 3.38, 3.5, 3.52, 4.18, 4.52, 4.0, 3.92, 3.76, 3.93, 3.74, 3.43, 3.47, 3.55, 3.74, 3.53, 3.36, 3.7, 3.7, 3.75, 3.82, 3.87, 3.95, 3.86, 4.42, 4.35, 4.13, 4.13, 4.14, 4.23, 4.32, 4.38, 4.35, 4.35, 5.33, 5.51, 6.06, 7.45, 7.34, 7.2, 7.41, 7.27, 6.21, 6.03, 6.35, 6.01, 6.44, 6.57, 6.58, 6.59, 7.26, 4.66, 5.12, 5.02, 4.86, 4.98, 5.22, 5.82, 5.74, 5.79, 6.41, 6.77, 6.9, 7.05, 7.49, 7.1, 7.35, 7.65, 8.44, 8.98, 9.18, 8.91, 9.2, 8.6, 6.97, 7.14, 8.12, 6.01, 5.87, 6.27, 6.1, 6.62, 6.98, 7.21, 7.22, 8.32, 8.09, 7.49, 8.1, 7.8, 7.15, 7.15, 6.82, 6.68, 6.93, 7.2, 6.92, 7.06, 6.91, 7.28, 7.49, 7.9, 8.1, 8.44, 8.48, 7.69, 7.61, 8.19, 9.68, 9.4, 9.98, 9.9, 8.2, 8.81, 7.38, 7.31, 6.41, 7.7, 8.18, 7.09, 7.17, 6.71, 6.17, 5.8, 5.9, 6.5, 6.98, 6.91, 7.1, 6.5, 5.85, 4.98, 4.3, 4.35, 4.4, 4.2, 3.34, 3.37, 3.13, 2.69, 2.73, 2.58, 2.44, 2.73, 3.08, 2.96, 3.66, 3.75, 3.78, 3.43, 3.2, 3.5, 3.71, 3.97, 4.49, 4.67, 4.99, 4.78, 4.91, 5.0, 5.43, 5.23, 5.42, 5.5, 5.82, 6.2, 6.61, 6.76, 7.3, 7.23, 7.0, 7.12, 7.05, 7.05, 7.79, 7.62, 7.31, 8.05, 7.2, 6.3, 6.23, 6.02, 6.55, 7.3, 7.13, 6.48, 6.97, 7.15, 8.02, 8.5, 8.96, 10.09, 10.49, 10.66, 9.31, 10.1, 10.57, 10.7, 11.74, 12.53, 12.77, 13.68, 12.98, 12.01, 11.03, 10.82, 11.02, 9.24, 9.4, 9.15, 8.46, 8.2, 7.87, 6.74, 6.9, 7.02, 7.03, 8.05, 8.07, 8.01, 8.32, 8.5, 8.75, 8.81, 9.05, 9.8, 9.06, 8.18, 8.21, 8.18, 8.69, 8.3, 7.82, 8.01, 7.2, 7.45, 7.66, 7.22, 7.78, 7.95, 8.4, 7.77, 8.01, 8.08, 8.1, 8.68, 8.63, 8.58, 9.08, 8.8, 8.78, 8.95, 9.2, 9.24, 9.43, 9.85, 9.41, 8.79, 8.86, 8.95, 8.87, 8.97, 9.12, 10.27, 10.93, 11.03, 10.45, 10.07, 10.5, 10.5, 9.92, 9.06, 9.22, 8.17, 7.62, 7.11, 7.45, 8.72, 9.1, 9.03, 9.92, 10.35, 6.4, 6.85, 
7.42, 7.45, 7.8, 7.88, 8.23, 8.53, 9.08, 9.4, 9.8, 10.0, 9.35, 10.4, 10.48, 11.0, 10.76, 10.95, 12.3, 12.53, 12.4, 12.77, 13.31, 13.11, 10.92, 5.8, 5.6, 5.21, 5.8, 5.8, 5.91, 6.04, 5.7, 5.25, 5.3, 5.6, 4.9, 4.7, 4.92, 5.05, 5.01, 5.08, 5.32, 5.06, 5.04, 4.32, 4.03, 4.87, 5.1, 5.06, 5.67, 5.9, 5.93, 5.73, 5.8, 5.85, 5.6, 5.34, 4.77, 5.57, 6.04, 5.68, 5.23, 5.63, 5.15, 5.5, 5.59, 5.68, 5.95, 6.18, 5.7, 5.48, 5.27, 4.34, 4.74, 5.05, 4.88, 4.92, 4.89, 5.02, 4.66, 4.68, 4.58, 4.35, 4.11, 4.2, 4.22, 4.32, 4.26, 4.17, 4.24, 4.26, 4.21, 4.22, 3.85, 3.82, 3.88, 3.91, 4.02, 4.14, 3.86, 3.91, 3.78, 3.8, 4.21, 4.31, 4.3, 4.08, 3.9, 3.78, 3.7, 3.7, 3.73, 3.56, 3.52, 3.44, 3.46, 3.54, 3.58, 3.93, 3.94, 4.04, 4.01, 4.07, 4.14, 4.22, 4.45, 4.85, 4.7, 4.82, 5.3, 5.32, 4.9, 4.99, 5.4, 5.6, 6.0, 6.53, 6.37, 5.98, 6.17, 5.62, 5.39, 5.05, 4.92, 5.38, 5.29, 5.28, 5.57, 6.33, 6.76, 7.17, 7.48, 7.79, 8.22, 8.79, 8.63, 8.03, 8.35, 8.51, 9.4, 10.42, 12.76, 12.98, 10.41, 7.12, 5.8, 7.56, 6.7, 6.62, 8.2, 7.18, 5.13, 4.85, 4.65, 5.04, 5.08, 5.66, 6.21, 6.61, 6.5, 6.67, 6.63, 6.88, 7.22, 7.78, 7.77, 7.91, 5.21, 4.8, 4.75, 4.24, 4.24, 4.36, 4.38, 4.24, 4.64, 4.6, 5.05, 5.18, 5.34, 5.72, 5.17, 5.15, 5.1, 4.6, 4.41, 4.47, 4.55, 4.49, 4.45, 4.51, 4.75, 4.78, 4.86, 4.68, 4.61, 4.95, 5.13, 5.08, 5.11, 5.15, 5.21, 5.89, 5.81, 6.23, 6.13, 5.8, 5.75, 6.03, 5.91, 5.75, 5.6, 5.3, 5.45, 5.35, 5.45, 5.18, 4.75, 5.04, 5.1, 5.19, 5.19, 5.23, 5.24, 5.31, 5.03, 4.86, 4.94, 4.97, 4.63, 4.29, 4.33, 3.88, 3.94, 3.67, 3.79, 3.87, 3.88, 3.86, 3.89, 3.97, 4.0, 3.97, 4.18, 4.22, 4.12, 4.2, 4.5, 4.6, 4.56, 4.54, 4.55, 4.35, 4.13, 4.1, 4.14, 3.99, 4.0, 3.84, 3.66, 3.7, 3.61, 3.63, 3.51, 3.5, 3.56, 3.8, 3.84, 3.87, 3.31, 3.2, 3.34, 3.45, 3.55, 3.55, 3.23, 3.12, 3.25, 3.39, 3.36, 3.32, 3.35, 3.42, 3.47, 3.66, 3.65, 3.83, 3.44, 3.3, 3.27, 
3.29, 3.28, 3.29, 3.17, 3.19, 3.08, 3.01, 3.01, 3.0, 2.99, 2.96, 2.96, 2.36, 2.22, 2.39, 2.47, 2.62, 2.69, 2.72, 2.61, 2.69, 2.71, 2.65, 2.51, 2.57, 2.67, 2.69, 2.64, 2.52, 2.61, 2.72, 2.91, 3.12, 3.11, 3.2, 3.16, 3.62, 3.55, 3.34, 2.81, 2.86, 2.7, 2.76, 2.72, 2.74, 2.75, 2.8, 2.68, 2.64, 2.64, 2.49, 
2.3, 2.32, 2.43, 2.39, 2.4, 2.49, 2.4, 2.39, 2.44, 2.4, 2.47, 2.55, 2.49, 2.45, 2.47, 2.54, 2.68, 2.7, 2.7, 2.84, 2.95, 2.9, 2.55, 2.68, 2.76, 2.78, 2.8, 2.77, 2.76, 2.68, 2.75, 2.8, 2.79, 2.96, 2.86, 2.93, 2.87, 2.77, 2.78, 2.7, 2.8, 2.71, 3.1, 3.07, 3.1, 3.03, 3.38, 3.53, 3.99, 3.96, 3.99, 3.7, 3.63, 3.75, 3.73, 3.75, 3.43, 3.19, 3.13, 3.33, 3.5, 3.77, 3.76, 3.56, 3.17, 3.1, 2.84, 2.87, 2.7, 2.51, 2.32, 2.38, 2.59, 2.62, 2.51, 2.6, 2.54, 2.51, 2.55, 2.54, 2.62, 2.6, 2.66, 2.71, 2.89, 2.82, 2.76, 2.7, 2.52, 2.53, 2.54, 2.47, 2.48, 2.45, 2.26, 2.31, 2.34, 2.29, 2.28, 2.32, 2.42, 2.41, 2.38, 2.37, 
2.36, 2.34, 2.2]
    for i in range(1, len(lowest_price_list) + 1):
        j = -1 * i
        #print(i, j)
        if week_dict["low"] > lowest_price_list[j]:
            week_dict["lowest_days"] = i - 1
            break
    if week_dict["lowest_days"] == -1:
        week_dict["lowest_days"] = len(lowest_price_list)
    print(week_dict["lowest_days"])

def main():
    #market = sys.argv[1]
    stock_code = sys.argv[1]
    transform_data(stock_code)
    day2week(stock_code)

if __name__ == '__main__':
    main()
    #test()
    