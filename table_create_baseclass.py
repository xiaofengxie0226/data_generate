"""
Authors

xiexiaofeng0226@gmail.com


Usage

Create fake data


Version

1.0.0

"""

import datetime
import random
import string
import time
import pandas as pd
import numpy as np


class TableCreate():
    def __init__(self,
                 table_name="Str"
                 ):
        self.table_name = table_name

    def dfCreateBlank(self, columns=None):
        if columns is None:
            columns = []
        df = pd.DataFrame(columns=columns)

        return df

    def dfCreate(self, columns=None,
                 id_range_start=1,
                 id_range_end=1001,
                 prefix="",
                 sl=8,
                 rows=10):
        if columns is None:
            columns = []
        df = pd.DataFrame(columns=columns)

        for column in columns:
            column_data = []
            if "id" in column:
                """
                userid: 1~1000/ productid: 1~10
                sex: 0(man),1(women)
                city: 1~47
                """
                if "sex" in column:
                    column_data = np.random.randint(2, size=rows)
                elif "city" in column:
                    column_data = np.random.randint(1, 48, size=rows)
                else:
                    column_data = random.sample(range(id_range_start, id_range_end), rows)
                    column_data = [prefix + str(i) for i in column_data]
            elif "time" in column:
                """
                action_time
                """
                otime = datetime.datetime.now()
                increment = datetime.timedelta(seconds=random.randint(30, 300))
                otime += increment
                column_data.append(otime)
            elif "day" in column:
                """
                birthday: 1980/1/1~2010/12/31
                """
                start = (1980, 1, 1, 0, 0, 0, 0, 0, 0)  # 设置开始日期时间元组
                end = (2010, 12, 31, 23, 59, 59, 0, 0, 0)  # 设置结束日期时间元组
                start_time = time.mktime(start)
                end_time = time.mktime(end)
                for i in range(rows):
                    t = random.randint(start_time, end_time)  # 在开始和结束时间戳中随机取出一个
                    date_tou = time.localtime(t)  # 将时间戳生成时间元组
                    birthday = time.strftime("%Y-%m-%d", date_tou)  # 将时间元组转成格式化字符串
                    column_data.append(birthday)
            else:
                """
                string.ascii_letters: A~Z. a~z 
                string.digits: 0~9
                StringLength = sl
                """
                for i in range(rows):
                    string_lst = ''.join(random.sample(string.ascii_letters, sl))
                    column_data.append(string_lst)

            df[column] = column_data

        return df

    def logCreate(self, user_df=pd.DataFrame, product_df=pd.DataFrame, columns=[]):
        df = self.dfCreateBlank(columns=columns)
        user_id_lst = user_df["user_id"].values.tolist()
        product_id_lst = product_df["product_id"].values.tolist()
        product_name_lst = product_df["product_name"].values.tolist()
        page_id_lst = [1, 2, 3, 4, 5]
        """
        session_id
        sessionLength <= 30
        """
        sessionlength_lst = np.random.randint(1, 31, size=len(user_id_lst)).tolist()
        for s in sessionlength_lst:
            session_id = ''.join(random.sample(string.ascii_letters + string.digits, 16))
            user_id = random.choice(user_id_lst)
            starttime = datetime.datetime.now()
            increment = datetime.timedelta(hours=random.randint(1, 24))
            endtime = starttime + increment
            for i in range(s):
                page_id = random.choice(page_id_lst)
                action_time = self.randomtimes(start=starttime, end=endtime, n=1)
                if page_id in [4, 5]:
                    search_product_name = "null"
                else:
                    search_product_name = random.choice(product_name_lst)
                click_product_id = random.choice(product_id_lst)
                try:
                    bucket_list = random.sample(product_id_lst, random.randint(0, 11))
                except ValueError:
                    bucket_list = []
                if page_id in [4, 5]:
                    order_list = random.sample(product_id_lst, random.randint(1, len(product_id_lst)))
                    for i in order_list:
                        if i in bucket_list:
                            bucket_list.remove(i)
                else:
                    order_list = []
                if page_id == 5 and len(order_list) > 0:
                    pay_time = action_time
                else:
                    pay_time = "null"
                # insert into df with list
                row = [user_id, session_id, page_id, action_time, search_product_name, click_product_id,
                       bucket_list, order_list, pay_time]
                df.loc[len(df)] = row

        print(len(df))

        return df

    def randomtimes(self, start, end, n, frmt="%Y-%m-%d %H:%M:%S"):
        time_datetime = [random.random() * (start - end) + start for _ in range(n)]
        time_str = [t.strftime(frmt) for t in time_datetime]
        return time_str


random.seed(15)

master_table = TableCreate("master_table")
user_info_lst = ["user_id", "username", "name", "sex_id", "birthday", "city_id"]
product_info_lst = ["product_id", "product_name", "extend_info"]
user_info = master_table.dfCreate(columns=user_info_lst, prefix="M", rows=1000)
product_info = master_table.dfCreate(columns=product_info_lst, prefix="P", id_range_end=11, rows=10)

user_info.to_json("user_info.json.gz", orient='records', compression="gzip")
product_info.to_json("product_info.json.gz", orient='records', compression="gzip")

user_info.to_csv("user_info.csv.gz", index=False, compression="gzip")
product_info.to_csv("product_info.csv.gz", index=False, compression="gzip")

Log = TableCreate()
log_lst = ["user_id", "session_id", "page_id", "action_time", "search_product_name", "click_product_id",
           "bucket_list", "order_list", "pay_time"]
for i in range(10):
    user_log = Log.logCreate(user_df=user_info, product_df=product_info, columns=log_lst)
    today = datetime.datetime.now()
    user_log.to_json(f"user_log({i}).json.gz", orient='records',compression="gzip")


print(user_info.head())
print(product_info.head())
print(user_log.head())
