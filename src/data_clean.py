"""数据清洗"""

import pandas as pd
import os
import datetime
import mysql.connector
import numpy as np

class DataCleaner:
    def __init__(self, df):
        self.raw_df = df
        self.cleaned_df = None
        self.cleaned_df_final = None
        self.abnormal_stats = {
            "purchase_gt_click": 0,
            "no_click_before_purchase": 0
        }

    def clean(self):
        """执行数据清洗流程"""
        print("\n开始数据清洗")
        self.cleaned_df = self.raw_df
        # 1. 缺失值处理
        self._handle_missing_values()

        # 2. 重复值处理
        self._handle_duplicates()

        # 3. 时间戳转换
        self._process_timestamps()

        # 4. 异常值处理
        self._handle_outliers()

        # 5. 行为类型转换
        self._convert_behavior_type()

        # 6. 行为逻辑验证
        self._validate_behavior_logic()

        # 保存清洗后的数据
        self._save_cleaned_data() # 数据已保存
        self._save_cleaned_to_db(table_name = "cleaned_data") # 数据已保存
        print("数据已清洗且保存！")
        return self.cleaned_df_final

    def _handle_missing_values(self):
        """处理缺失值"""
        missing_counts = self.cleaned_df.isnull().sum()
        print("\n缺失值情况：")
        print(missing_counts)

        if missing_counts.sum() > 0:
            self.cleaned_df.dropna(inplace = True)
            print(f"删除缺失值后数据形状：{self.cleaned_df.shape}")

    def _handle_duplicates(self):
        """处理重复值"""
        duplicate_counts = self.cleaned_df.duplicated().sum()
        print("\n重复值情况：")
        print(duplicate_counts)

        if duplicate_counts > 0:
            self.cleaned_df.drop_duplicates(inplace = True)
            print(f"去重后数据形状：{self.cleaned_df.shape}")

    def _process_timestamps(self):
        """处理时间戳,提取日期特征"""
        print("\n转换为datetime的时间格式...")
        # 转换时间戳为datetime格式
        self.cleaned_df["time"] = pd.to_datetime(self.cleaned_df["time"],unit="s")

        # 提取日期特征
        self.cleaned_df["date"] = self.cleaned_df["time"].dt.date
        self.cleaned_df["hour"] = self.cleaned_df["time"].dt.hour
        self.cleaned_df["weekday"] =self.cleaned_df["time"].dt.weekday # 0=周一，6=周日

    def _handle_outliers(self):
        """处理异常值，过滤时间范围"""
        # 从data_exploration.txt能看出时间戳含有异常值（负数时间戳）
        # 查看数据时间范围
        min_date = self.cleaned_df["time"].min()
        max_date = self.cleaned_df["time"].max()
        print(f"\n数据时间范围：{min_date} 至 {max_date}")

        start_date = pd.to_datetime("2017-11-25") # timestamp类型
        end_date = pd.to_datetime("2017-12-04")
        self.cleaned_df = self.cleaned_df[
            (self.cleaned_df["time"] >=start_date) & # datetimeseries类型
        (self.cleaned_df["time"] < end_date)
        ]
        print(f"过滤后数据形状：{self.cleaned_df.shape}")

    def _convert_behavior_type(self):
        """行为类型转换为中文"""
        # 定义映射字典behavior_map
        # 创建cleaned_df_final副本的目的是确保使用.map函数前df是独立的dataframe,而不是切片，否则出现警告
        self.cleaned_df_final = self.cleaned_df.copy()
        behavior_map = {"pv":"点击","buy":"购买","cart":"加购","fav":"收藏"}
        self.cleaned_df_final["behavior"] = self.cleaned_df_final["behavior_type"].map(behavior_map)
        self.cleaned_df_final["behavior"] = self.cleaned_df_final["behavior"].astype("category")

    def _save_cleaned_data(self):
        """保存清洗后的数据"""
        # 前置检查：确保数据存在
        if self.cleaned_df_final is None or self.cleaned_df_final.empty:
            print("清洗后的为空或者未完成清洗，无法保存")
            return False

        if not os.path.exists("data/processed"):
            os.makedirs("data/processed")

        self.cleaned_df_final.to_csv("data/processed/cleaned_data.csv",index = False,mode="w",header=False)
        print("清洗后的数据已保存至data/processed/cleaned_data.csv")

        if not os.path.exists("output/reports"):
            os.makedirs("output/reports")

        with open("output/reports/cleaned_data_exploration.txt","w",encoding= 'utf-8') as f:
            f.write("===== 数据基本信息 =====\n")
            self.cleaned_df_final.info(buf = f)
            f.write("\n===== 数据前5行 =====\n")
            f.write(str(self.cleaned_df_final.head()))
            f.write("\n===== 数据统计描述 =====\n")
            f.write(str(self.cleaned_df_final.describe()))
        print("清洗后的数据的基本信息已保存到output/reports/cleaned_data_exploration.txt\n")

    def _save_cleaned_to_db(self,table_name="cleaned_data"):
        """清洗后的数据保存到mysql数据库中"""
        # 前置检查：确保数据存在
        if self.cleaned_df_final is None or self.cleaned_df_final.empty:
            print("清洗后的为空或者未完成清洗，无法保存")
            return False
        # CSV文件路径
        cleaned_data_csv_path = "data/processed/cleaned_data.csv"
        if not os.path.exists(cleaned_data_csv_path):
            print("cleaned_data_csv_path路径不存在，请将清洗后的数据保存到该路径")
            return False
        file_size = os.path.getsize(cleaned_data_csv_path) / (1024 ** 3)  # GB
        print(f"找到CSV文件: {cleaned_data_csv_path} (大小: {file_size:.2f} GB)")

        # 数据库配置
        db_config = {
            "host":"localhost",
            "user":"lyuj",
            "password":"lyuj123456",
            "port":3306,
            "charset":"utf8mb4",
            "allow_local_infile":True # 必须启用本地文件加载(开启客户端(python)本地文件加载功能)
        }
        db_name = "ecommerce_db"


        conn = None
        cursor = None
        try:
            # 1. 数据库连接（先不指定数据库，因为可能需要创建）
            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor()

            # 检查数据库是否存在，不存在则创建
            cursor.execute(f"create database if not exists {db_name} character set utf8mb4 collate utf8mb4_unicode_ci")
            print(f"数据库{db_name}检查/创建完成")
            cursor.execute(f"use {db_name}")
            print(f"使用数据库{db_name}")

            # 2. 创建cleaned_data表
            create_final_table_sql = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            id INT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
                            user_id INT NOT NULL COMMENT '用户ID',
                            product_id INT NOT NULL COMMENT '商品ID',
                            product_category_id INT NOT NULL COMMENT '商品类目ID',
                            behavior_type VARCHAR(10) NOT NULL COMMENT '行为类型',
                            time DATETIME NOT NULL COMMENT '行为发生时间',
                            date DATE NOT NULL COMMENT '行为发生日期',
                            hour INT NOT NULL COMMENT '行为发生小时',
                            weekday INT NOT NULL COMMENT '星期几（0-6,0为周一）',
                            behavior VARCHAR(20) NOT NULL COMMENT '行为类型中文描述',
                            create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '数据入库时间',
                            INDEX idx_user (user_id),
                            INDEX idx_time (time),
                            INDEX idx_behavior (behavior_type)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='清洗后的数据';
                        """
            cursor.execute(create_final_table_sql)


            # 3. 使用批量导入数据语句load data
            load_data_sql = f"""
            load data local infile '{cleaned_data_csv_path}'
            into table {table_name} character set utf8mb4 
            fields terminated by ',' -- 逗号分隔
            lines terminated by '\\n' -- 标准换行符
            (user_id,product_id, product_category_id,
            behavior_type, time, date, hour, weekday, behavior);
            """
            print("开始带入数据...")
            # cursor.execute("set global local_infile = 1;")
            cursor.execute(load_data_sql)
            conn.commit()
            print(f"成功导入{cursor.rowcount}条数据") # cursor.rowcount为影响行数
            conn.commit()
            print(f"导入完成，cleaned_data表数据量：{cursor.rowcount} 行")
            return True


        except mysql.connector.Error as e:
                print(f"数据库操作错误：{str(e)}")
                # 出错时回滚事务
                if conn and conn.is_connected():
                    conn.rollback()

                # 提供详细的错误信息
                import traceback
                print("错误详情：")
                print(traceback.format_exc()) # 打印错误类型、消息、发生文件/行号、调用栈等
                return False

        except Exception as e:
            print(f"系统错误：{str(e)}")
            return False

        finally:
            # 关闭数据库连接
            if cursor:
                cursor.close()
            if conn and conn.is_connected():
                conn.close()
                print("数据库连接已关闭")

    def _validate_behavior_logic(self):
        """解决数据质量问题"""
        print("\n检测购买量和点击量是否有问题...")

        # 1. 解决购买前无点击问题（删除无点击的购买记录）
        # 获取所有购买记录
        purchases = self.cleaned_df_final[(self.cleaned_df_final['behavior'] == '购买')]

        if not purchases.empty:
            # 找出没有前置点击的购买记录
            # 使用分组聚合查找每个用户-商品组合的首次点击时间
            min_click_times = self.cleaned_df_final[
                self.cleaned_df_final['behavior'] == '点击'
                ].groupby(['user_id', 'product_id'])['time'].min().reset_index(name='min_click_time')


            # 合并购买记录和首次点击时间
            # 保存原始索引
            purchases_with_index = purchases.reset_index().rename(columns={'index': 'original_index'})
            merged = purchases_with_index.merge(
                min_click_times,
                on=['user_id', 'product_id'],
                how='left'
            )

            # 找出没有点击或点击时间晚于购买时间的记录
            no_click_purchases = merged[
                (merged['min_click_time'].isna()) |
                (merged['min_click_time'] > merged['time'])
                ]

            # 删除这些购买记录
            if not no_click_purchases.empty:
                # 提取原始索引
                remove_indices = no_click_purchases["original_index"]
                # 删除
                self.cleaned_df_final = self.cleaned_df_final.drop(remove_indices)
                print(f"已删除 {len(no_click_purchases)} 条没有前置点击的购买记录")
                print(f"删除后数据形状：{self.cleaned_df_final.shape}")


        # 2. 检测购买量是否大于点击量
        # 计算每个类目-行为组合的数量
        category_behavior = self.cleaned_df_final.groupby(
            ['product_category_id', 'behavior'],observed=False
        ).size().unstack(fill_value=0)

        # 找出购买量 > 点击量的类目
        problem_categories = category_behavior[
            ((category_behavior['购买'] > category_behavior['点击']) &
            (category_behavior['点击'] > 0))
            ]
        if not problem_categories.empty:
            print(f"发现 {len(problem_categories)} 个类目存在购买量 > 点击量问题\n")
        else:
            print("没有发现类目存在购买量 > 点击量问题 \n")