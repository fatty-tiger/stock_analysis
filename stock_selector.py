import pandas as pd
import sys

def select_test1(stock_code):
    """
    查找某只股票，周K涨幅>=30%的日期，返回日期列表（后改为周k数据）
    """
    df = pd.read_csv("./data/tdx/week/%s.csv" % stock_code)
    #print(df.head(10))
    
    # df2 = df[(df["increase_r"] >= 0.2) & (df["special"] >= 52)]
    df2 = df[df["increase_r"] >= 0.25]

    print(df2)
    #df2.to_csv("./data/tmp/test.csv", index=None)


if __name__ == "__main__":
    stock_code = sys.argv[1]
    select_test1(stock_code)