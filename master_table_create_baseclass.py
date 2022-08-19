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

    def dfCreate(self, columns=None,
                 id_range_start=1,
                 id_range_end=1000,
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
                sex: 0,1
                city: 1~47
                """
                if "sex" in column:
                    column_data = np.random.randint(2, size=rows)
                elif "city" in column:
                    column_data = np.random.randint(1, 47, size=rows)
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
                string.ascii_letters = 26个小写,26个大写 
                随机生成sl长度的字符串
                """
                for i in range(rows):
                    string_lst = ''.join(random.sample(string.ascii_letters, sl))
                    column_data.append(string_lst)

            df[column] = column_data

        return df


random.seed(15)

master_table = TableCreate("master_table")
user_info_lst = ["user_id", "username", "name", "sex_id", "birthday", "city_id"]
product_info_lst = ["product_id", "product_name", "extend_info"]
user_info = master_table.dfCreate(columns=user_info_lst, prefix="M", rows=100)
product_info = master_table.dfCreate(columns=product_info_lst, prefix="P", rows=26)

# print(user_info.head())
# print(product_info.head())

user_info.to_json("user_info.json", orient='records')
product_info.to_json("product_info.json", orient='records')
