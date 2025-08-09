# 电商销售数据分析项目方案

## 一、项目概述
### 1.1 项目背景
随着电商行业的快速发展，企业积累了海量的用户行为和交易数据。通过对这些数据的深度分析，能够挖掘用户消费习惯、优化产品结构、提升营销效果，为企业决策提供数据支持。本项目基于淘宝用户行为数据，在PyCharm环境中完成全流程数据分析与挖掘。

### 1.2 项目目标
- 掌握完整的数据分析流程（数据获取→清洗→分析→可视化→结论）
- 挖掘用户行为模式与消费特征
- 构建用户价值评估模型（RFM模型）
- 提出可落地的业务优化建议
- 形成专业的数据分析报告

### 1.3 项目价值
体现数据处理、可视化分析、用户画像构建等实战能力，展示对电商业务的理解，为简历增添实质性项目经验。

## 二、环境准备
### 2.1 所需工具
- Python 3.8+（数据分析核心工具）
- PyCharm（代码编写与运行环境）
- 相关库：pandas、numpy、matplotlib、seaborn、pyecharts

### 2.2 环境配置步骤
1. **创建虚拟环境**（在PyCharm终端执行）：# 创建虚拟环境
python -m venv ecommerce_env

# 激活虚拟环境
# Windows系统
ecommerce_env\Scripts\activate
# Mac/Linux系统
source ecommerce_env/bin/activate

# 安装所需库
pip install pandas numpy matplotlib seaborn pyecharts sqlalchemy openpyxl
2. **PyCharm项目设置**：
   - 打开PyCharm → `File` → `New Project`
   - 选择项目路径，勾选"Existing interpreter"
   - 选择刚刚创建的虚拟环境中的python.exe
   - 点击"Create"完成项目创建

3. **项目目录结构**：ecommerce_analysis/
├── data/               # 数据文件目录
│   ├── raw/            # 原始数据
│   └── processed/      # 清洗后数据
├── src/                # 源代码目录
│   ├── __init__.py
│   ├── data_cleaning.py  # 数据清洗模块
│   ├── eda.py           # 探索性分析模块
│   └── rfm_analysis.py  # RFM分析模块
├── output/             # 输出文件目录
│   ├── charts/         # 图表输出
│   └── reports/        # 报告文件
└── main.py             # 主程序入口
4. **核心库导入代码**：import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pyecharts.charts import Bar, Line, Pie, Funnel
from pyecharts import options as opts
from pyecharts.render import render
import datetime
import os
import mysql.connector
from mysql.connector import Error
5. **中文显示配置**：# 设置matplotlib中文显示
plt.rcParams["font.family"] = ["SimHei", "WenQuanYi Micro Hei", "Heiti TC"]
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

# 设置seaborn中文显示
sns.set(font='SimHei', font_scale=1.2)
sns.set_style("whitegrid")
## 三、数据处理与分析过程

### 3.1 数据获取
- 数据源：阿里天池"淘宝用户行为数据集"（https://tianchi.aliyun.com/dataset/dataDetail?dataId=649 ）
- 数据规模：100万条用户行为记录
- 数据字段：user_id（用户ID）、item_id（商品ID）、category_id（商品类目ID）、behavior_type（行为类型：1-点击，2-收藏，3-加购，4-购买）、timestamp（时间戳）

- **数据下载与存储**：
  1. 从上述链接下载数据集，保存至`data/raw/`目录
  2. 解压文件，重命名为`user_behaviors.csv`

- 
### 3.2 数据导入与初步探索
创建`src/data_exploration.py`文件：
"""数据读取和初步探索"""
import pandas as pd
import os

class DataExplorer:
    def __init__(self,data_path):
        self.data_path = data_path
        self.df = None

    def read_data(self):
        """读取数据，限制最大读取行数"""
        try:
            self.df = pd.read_csv(self.data_path,header=None,nrows=1000000)
            self.df.columns = ["user_id","product_id","product_category_id","behavior_type","time"]
            print(f"数据读取成功，共{self.df.shape[0]}行，{self.df.shape[1]}列")
            return True
        except Exception as e:
            print(f"数据读取失败：{str(e)}")
            return False
    def basic_exploration(self):
        """基本数据探索"""
        if self.df is None:
            print("请先加载数据")
            return

        print("\n===== 数据基本信息 =====")
        print(self.df.info())

        print("\n===== 数据前5行 =====")
        print(self.df.head())

        print("\n===== 数据统计描述 =====")
        print(self.df.describe())

        print("\n===== 用户行为分布 =====")
        behavior_counts = self.df["behavior_type"].value_counts()
        print(behavior_counts)

        # 保存探索结果
        self._save_exploration_results(behavior_counts) # 数据已保存

        return self.df

    def _save_exploration_results(self,behavior_counts):
        """保存探索结果到文件"""
        # 确保路径存在
        if not os.path.exists("output/reports"):
            os.makedirs("output/reports")

        with open("output/reports/data_exploration.txt",
                  "w",encoding="utf - 8") as f:
            f.write("===== 数据基本信息 =====\n")
            self.df.info(buf = f)
            f.write("\n===== 数据前5行 =====\n")
            f.write(str(self.df.head()))
            f.write("\n===== 数据统计描述 =====\n")
            f.write(str(self.df.describe()))
            f.write("\n===== 行为类型分布 =====\n")
            f.write(str(behavior_counts))

# 在main.py中调用
if __name__ == "__main__":
    explorer = DataExplorer('data/raw/user_behaviors.csv')
    if explorer.load_data():
        df = explorer.basic_exploration()
### 3.3 数据清洗
创建`src/data_cleaning.py`文件：
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



# 在main.py中调用
if __name__ == "__main__":
    # 假设已经加载了原始数据
    from data_exploration import DataExplorer
    explorer = DataExplorer('data/raw/user_behaviors.csv')
    if explorer.load_data():
        raw_df = explorer.basic_exploration()
        cleaner = DataCleaner(raw_df)
        cleaned_df = cleaner.clean()
### 3.4 探索性数据分析（EDA）
创建`src/eda.py`文件：
import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns
import os
import numpy as np
from pyecharts.charts import Bar,Line,Pie,Funnel
from pyecharts import options as opts

class EDAnalyzer:

    # 设置全局正文字体
    plt.rcParams["font.family"] = ["SimHei"]

    def __init__(self,df):
        self.df = df
        # 创建输出目录
        self._create_output_dirs()

    def _create_output_dirs(self):
        """创建输出目录"""
        dirs = ["output/charts","output/reports"]
        for dir in dirs:
            if not os.path.exists(dir):
                os.makedirs(dir)


    def overall_analysis(self):
        """整体数据概览分析"""
        print("\n===== 开始整体数据概览分析 =====")

        # 行为类型分布
        behavior_counts = self.df["behavior"].value_counts()

        # 绘制行为类型分布柱状图
        plt.figure(figsize=(10,6))
        plt.bar(behavior_counts.index,behavior_counts.values,width=0.4)
        plt.xlabel("行为类型")
        plt.ylabel("数量")
        plt.title("用户行为类型分布")
        for i,v in enumerate(behavior_counts):
            plt.text(i,v + 0.5,str(v),ha="center",fontsize =12)
        plt.tight_layout()
        plt.savefig("output/charts/behavior_dirstribution.png",dpi=300)
        plt.close()
        print("用户行为类型分布图已保存")

        # 计算各行为的转化率
        total_click = behavior_counts["点击"]
        collect_rate = behavior_counts["收藏"] / total_click if '收藏' in behavior_counts else 0
        cart_rate = behavior_counts["加购"] / total_click if '加购' in behavior_counts else 0
        purchase_rate = behavior_counts["购买"] / total_click if '购买' in behavior_counts else 0

        # 保存转化率结果
        with open("output/reports/conversion_rates.txt","w",encoding="utf-8") as f:
            f.write(f"点击到收藏的转化率：{collect_rate:.2%}\n")
            f.write(f"点击到加购的转化率：{cart_rate:.2%}\n")
            f.write(f"点击到购买的转化率：{purchase_rate:.2%}\n")

    def time_based_anlysis(self):
        """时间维度分析（双Y轴）"""
        print("\n===== 开始时间维度分析 =====")

        # 1.每日用户行为趋势（日维度）
        daily_behavior = self.df.groupby(["date","behavior"],observed=True).size().unstack() # 宽表转长表

        plt.figure(figsize=(10,6))
        ax1 = plt.gca() # 主坐标轴
        ax2 = ax1.twinx() # 次坐标轴

        # 设置Y轴范围
        right_click_max = daily_behavior["点击"].max() * 1.1
        left_clicj_max = daily_behavior[["加购","收藏","购买"]].max().max() * 1.2

        # 绘制三个行为的折线图（左侧）
        for behavior in ["加购","收藏","购买"]:
            ax1.plot(daily_behavior.index,daily_behavior[behavior],
            marker="o",label=behavior)

        # 绘制点击的折线图（右侧）
        ax2.plot(daily_behavior.index,daily_behavior["点击"],
            linestyle="--",marker="s",label="点击（右轴）",
            color="purple")

        # 设置左侧Y轴
        ax1.set_ylim(0,left_clicj_max)
        ax1.set_ylabel("加购/收藏/购买数量",fontsize=12)
        ax1.grid(True,alpha=0.3)
        ax1.legend(loc="upper left")

        # 设置右侧Y轴
        ax2.set_ylim(0,right_click_max)
        ax2.set_ylabel("点击",fontsize=12,color="purple")
        ax2.tick_params(axis="y",labelcolor="purple") # 作用与Y轴刻度

        # 设置公共参数
        plt.title("每日用户行为趋势",fontsize=14,pad=20)
        ax1.set_xlabel("日期",fontsize=12)
        # 旋转45°C： ax1.set_xticklabels(ax1.get_xticklabels(),rotation=45,ha="right")
        plt.xticks(ha="right")

        # 合并图例
        lines1,labels1 = ax1.get_legend_handles_labels()
        lines2,labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2,labels1 + labels2,loc="best")

        plt.tight_layout()
        plt.savefig("output/charts/daily_behavior_trend.png",dpi=300)
        plt.close()
        print("每日用户行为趋势图已保存")

        # 2.小时段行为分布（小时维度）
        hourly_behavior = self.df.groupby(["hour","behavior"],observed=True)["behavior"].count().unstack()

        plt.figure(figsize=(12,6))
        ax1 = plt.gca() # 主坐标轴
        ax2 = ax1.twinx() # 次坐标轴

        # 绘制点击的折线图
        ax2.plot(hourly_behavior.index,hourly_behavior["点击"],
                 linestyle="--",label="点击（右轴）",color="purple")

        # 绘制其他行为的折线图
        for behavior in ["加购","购买","收藏"]:
            ax1.plot(hourly_behavior.index,hourly_behavior[behavior],label=behavior)

        # 设置Y轴范围
        left_y_max = hourly_behavior[["加购","购买","收藏"]].max().max() * 1.3
        right_y_max = hourly_behavior["点击"].max() * 1.1

        # 设置左侧Y轴
        ax1.set_ylim(0,left_y_max)
        ax1.set_ylabel("加购/收藏/购买数量",fontsize=10)
        ax1.legend(loc="upper left")
        ax1.grid(True,alpha=0.4)
        # 设置峰值(annotate标注函数)
        ax1.annotate(f'加购峰值：{hourly_behavior["加购"].max()}（14点）',
                     xy=(14, hourly_behavior["加购"].max()),
                     xytext=(15, 5000),
                     arrowprops=dict(arrowstyle="->", color="b"))

        # 设置右侧Y轴
        ax2.set_ylim(0,right_y_max)
        ax2.set_ylabel("点击数量",fontsize=10,color="purple")
        ax2.tick_params(axis = "y",labelcolor="purple")
        ax2.annotate(f'点击峰值：{hourly_behavior["点击"].max()}（13点）',
                     xy=(13,hourly_behavior["点击"].max()),
                     xytext=(10,77000),
                     arrowprops=dict(arrowstyle="->",color="purple"))


        # 设置全局参数
        plt.title("用户行为小时段分布",pad=20)
        ax1.set_xlabel("小时",fontsize=10)
        _x = range(24)
        plt.xticks(np.arange(24))
        plt.xlim(0,23)
        plt.tight_layout()

        # 组合图例
        lines1,labels1 = ax1.get_legend_handles_labels()
        lines2,labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2,labels1 + labels2,loc = "best")

        # 保存为图片
        plt.savefig("output/charts/hourly_behavior_distribution.png",dpi=300)
        plt.close()
        print("小时段用户分析分布图已保存")


        # 3. 周内行为分布
        weekday_behavior = self.df.groupby(["weekday","behavior"],observed=True).size().unstack()
        weekday_behavior.index = ["周一","周二","周三","周四","周五","周六","周日"]

        plt.figure(figsize=(14,7))
        ax1 = plt.gca()
        ax2 = ax1.twinx()

        # 绘制左侧折线图(三个行为)
        for behavior in ["加购","购买","收藏"]:
            ax1.plot(weekday_behavior.index,weekday_behavior[behavior],label=behavior)

        # 绘制右侧折线图（点击）
        ax2.plot(weekday_behavior.index,weekday_behavior["点击"],linestyle="--",color="purple",label="点击（右侧）")

        # 获取Y轴最大刻度
        left_y_max = weekday_behavior[["加购","购买","收藏"]].max().max() * 1.3
        rigth_y_max = weekday_behavior["点击"].max() * 1.1

        # 设置左侧Y轴
        ax1.set_ylim(0,left_y_max)
        ax1.set_xlabel("星期",fontsize=12)
        ax1.set_ylabel("加购/购买/收藏数量",fontsize=12)
        ax1.grid(alpha=0.4)

        # 设置右侧Y轴
        ax2.set_ylim(0, rigth_y_max)
        ax2.set_ylabel("点击数量",fontsize=12,color="purple")
        ax2.tick_params(axis="y",labelcolor="purple")

        # 组合图例
        lines1,labels1 = ax1.get_legend_handles_labels()
        lines2,labels2 = ax2.get_legend_handles_labels()
        plt.legend(lines1 + lines2,labels1 + labels2,loc="best")

        # 设置全局参数
        plt.title("周内用户行为分布",fontsize=16,pad=20)
        plt.tight_layout()
        ax1.set_xlim(0,6)

        # 设置峰值
        ax1.annotate(f'加购峰值：{weekday_behavior["加购"].max()}',
                     xy=(5,weekday_behavior["加购"].max()),
                     xytext=(5.2,11500),
                     arrowprops=dict(arrowstyle="->",color="b")
                     )
        ax2.annotate(f'点击峰值：{weekday_behavior["点击"].max()}',
                     xy=(5, weekday_behavior["点击"].max()),
                     xytext=(4.3, 230000),
                     arrowprops=dict(arrowstyle="->", color="b")
                     )

        # 保存图片
        plt.savefig("output/charts/weekday_behavior_distribution.png",dpi=300)
        print("周内用户行为分析图已保存")

    def product_based_analysis(self):
        """商品维度分析"""
        print("\n===== 开始商品维度分析 =====")

        # 1.热门商品分析TOP10
        top10_product = self.df[self.df["behavior"] == "购买"].groupby("product_id").size().nlargest(10)

        # 绘制条形图
        plt.figure(figsize=(12, 6))
        plt.bar(top10_product.index.astype(str),top10_product.values,width = 0.6)
        plt.ylabel("购买次数",fontsize=12)
        plt.xlabel("商品ID",fontsize=12)
        plt.title("购买量TOP10商品",fontsize=14,pad=20)
        plt.tight_layout()

        # 添加标签
        for i,v in enumerate(top10_product.values):
            plt.text(i,v + 0.2,f'{v}',ha = "center",fontsize=12)

        # 保存图片
        plt.savefig("output/charts/top10_product.png",dpi=300)
        print("热门商品TOP10图已保存")

        # 2.热门商品类目分析
        top10_categories = self.df.groupby("product_category_id").size().nlargest(10)

        # 绘制条形图
        plt.figure(figsize=(12, 6))
        plt.bar(top10_categories.index.astype(str), top10_categories.values, width=0.6)
        plt.ylabel("购买次数", fontsize=12)
        plt.xlabel("商品类目ID", fontsize=12)
        plt.title("购买量TOP10商品类目", fontsize=14, pad=20)
        plt.xticks(range(len(top10_categories)),top10_categories.index)
        plt.tight_layout()

        # 添加标签
        for i, v in enumerate(top10_categories.values):
            plt.text(i, v + 0.2, f'{v}', ha = "center",fontsize=12)

        # 保存图片
        plt.savefig("output/charts/top10_categories.png",dpi=300)
        print("热门商品类目TOP10图已保存")


        # 3.top10商品类目转化率分析
        category_behavior = self.df.groupby(["product_category_id","behavior"],observed=True).size().unstack()

        # 计算各品类的购买转化率
        category_behavior["转化率"] = category_behavior["购买"] / category_behavior["点击"]
        # 去除转化率为NAN的记录
        category_behavior = category_behavior.dropna(subset=["转化率"])
        # 取转化率top10的品类
        top10_conversion = category_behavior["转化率"].sort_values(ascending=False).head(10)

        # 绘制条形图
        plt.figure(figsize=(12,6))
        top10_conversion.plot(kind = "barh")
        plt.xlabel("商品类目ID")
        plt.ylabel("转化率")
        plt.title("top10商品类目购买转化率")
        plt.tight_layout()
        plt.savefig("output/charts/top10_conversion.png",dpi = 300)
        plt.close()
        print("商品类目转化率top10图已保存")

    def user_behavior_path_analysis(self):
        """用户行为路径分析（漏斗图）"""
        print("\n===== 开始用户行为路径分析 =====")

        try:

            # 获取行为计数且排序
            funnel_data = self.df['behavior'].value_counts()[['点击', '加购', '收藏', '购买']]

            # 使用pyecharts绘制漏斗图
            funnel = Funnel(init_opts=opts.InitOpts(width="1000px", height="600px"))
            funnel.add(
                series_name="用户行为转化",
                data_pair=list(funnel_data.items()), # pandas Series通过items()返回(索引,值)元组
                gap=2,
                # 鼠标悬停提示框配置
                tooltip_opts=opts.TooltipOpts(
                    formatter="{b}: {c} ({d}%)"
                ),
                # 标签配置
                label_opts=opts.LabelOpts(
                    position="inside",
                    formatter="{b}: {c}\n占比: {d}%"
                ),
                sort_="descending"  # 关键设置：确保降序排列
            )

            # 设置全局配置
            funnel.set_global_opts(
                # 设置标题
                title_opts=opts.TitleOpts(
                    title="用户行为转化漏斗",
                    subtitle="点击 → 加购 → 收藏 → 购买",
                    pos_left="center"
                ),
                # 设置图例
                legend_opts=opts.LegendOpts(is_show=True,pos_left="left"),
                #
                tooltip_opts=opts.TooltipOpts(trigger="item", axis_pointer_type="shadow")
            )

            # 保存为HTML文件
            output_path = "output/charts/user_behavior_funnel.html"
            funnel.render(output_path)
            print(f"用户行为转化漏斗图已保存至: {output_path}")

            # 生成文本报告
            with open("output/reports/funnel_conversion_rates.txt", "w", encoding="utf-8") as f:
                f.write("用户行为转化分析报告\n")
                f.write("=" * 40 + "\n")
                f.write(f"{'行为':<8}{'数量':<10}{'占比(%)':<10}\n")
                f.write("-" * 40 + "\n")

                total = funnel_data.values.sum()
                for behavior, count in zip(funnel_data.index,funnel_data.values):
                    percentage = (count / total) * 100
                    f.write(f"{behavior:<8}{count:<10}{percentage:.2f}%\n")

                # 计算转化率
                if len(funnel_data) > 1:
                    f.write("\n用户路径转化率分析:\n")
                    f.write("=" * 40 + "\n")
                    for i in range(1, len(funnel_data)):
                        prev_count = funnel_data.iloc[i - 1]
                        current_count = funnel_data.iloc[i]
                        conversion_rate = (current_count / prev_count) * 100 if prev_count > 0 else 0
                        f.write(f"{funnel_data.index[i - 1]} → {funnel_data.index[i]}: {conversion_rate:.2f}%\n")

            print(f"漏斗分析报告已保存至:output/reports/funnel_conversion_rates.txt ")

        except Exception as e:
            print(f"绘制漏斗图时出错: {str(e)}")


    

# 在main.py中调用
if __name__ == "__main__":
    # 假设已经加载并清洗了数据
    cleaned_df = pd.read_csv('data/processed/cleaned_user_behaviors.csv')
    analyzer = EDAnalyzer(cleaned_df)
    analyzer.overall_analysis()
    analyzer.time_based_analysis()
    analyzer.product_based_analysis()
    analyzer.user_behavior_path_analysis()
### 3.5 用户价值分析（RFM模型）
创建`src/rfm_analysis.py`文件：
import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns
import os
from datetime import datetime

class RFMAnalyzer:

    # 设置全局中文字体
    plt.rcParams["font.family"] = ["SimHei"]

    def __init__(self,df):
        self.df = df
        self.rfm = None
        # 确保日期列是日期类型
        self.df["date"] = pd.to_datetime(self.df["date"])

        # 创建输出目录
        if not os.path.exists("output/charts"):
            os.makedirs("output/charts")

    def calculate_rfm(self):
        """计算RFM指标"""
        print("\n===== 开始RFM分析 =====")

        # 确定分析截止日期（数据的最后一天）
        last_date = self.df["date"].max()
        print(f"分析截止日期：{last_date}")

        # 1.计算RFM指标
        # R: 最近一次购买时间（天）
        rfm_r = self.df[self.df["behavior"] == "购买"].groupby("user_id")["date"].max()
        rfm_r = (last_date - rfm_r).dt.days

        # F: 购买频率
        rfm_f= self.df[self.df["behavior"] == "购买"].groupby("user_id").size()

        # 消费金额(由于数据及没有消费金额，只能用购买次数代替）
        rfm_m = self.df[self.df["behavior"] == "购买"].groupby("user_id").size()

        # 合并为RFM数据框
        self.rfm = pd.DataFrame({
            "R":rfm_r,
            "F":rfm_f,
            "M":rfm_m
        }).fillna(0)

        print("RFM指标计算完成")
        return self.rfm

    def score_rfm(self):
        """为RFM指标打分"""
        if self.rfm is None:
            print("请先计算RFM指标")
            return

        # 处理可能的异常值
        self.rfm = self.rfm[(self.rfm["F"] > 0) & (self.rfm["M"] > 0)]

        # RFM评分（1-5分，5分最佳）
        # 对于R，值越小越好，所以分箱时标签反转
        r_qcut = pd.qcut(self.rfm["R"],q=5,duplicates="drop")
        n_categories = len(r_qcut.cat.categories) # 获取实际分箱数量
        r_labels = list(range(n_categories,1-1,-1))
        self.rfm["R_score"] = pd.qcut(self.rfm["R"],
                                      5,
                                      labels = r_labels,
                                      duplicates="drop",
                                      ).astype(int)

        # 对于F和M，值越大越好
        f_qcut = pd.qcut(self.rfm["F"],q=5,duplicates="drop")
        n_categories = len(f_qcut.cat.categories)
        f_labels = list(range(1,n_categories + 1))
        self.rfm["F_score"] = pd.qcut(self.rfm["F"],
                                      5,
                                      labels=f_labels,
                                      duplicates="drop"
                                      ).astype(int)

        m_qcut = pd.qcut(self.rfm["M"], q=5, duplicates="drop")
        n_categories = len(m_qcut.cat.categories)
        m_labels = list(range(1, n_categories + 1))
        self.rfm["M_score"] = pd.qcut(self.rfm["M"],
                                      5,
                                      labels=m_labels,
                                      duplicates="drop"
                                      ).astype(int)

        # 计算RFM总分
        self.rfm["RFM_total"] = self.rfm["R_score"] + self.rfm["F_score"] + self.rfm["M_score"]
        print("RFM打分完成")

    def segment_users(self):
        """用户分群"""
        if self.rfm is None:
            print("请先计算并打分RFM指标")
            return

        # 定义用户分群函数
        def rfm_segment(row):
            if row["R_score"] >= 3 and row["F_score"] >= 3 and row["M_score"] >= 3:
                return "高价值用户"
            elif row["R_score"] >= 3 and row["F_score"] >= 2 and row["M_score"] >= 2:
                return "潜力用户"
            elif row["R_score"] >= 3 and row["F_score"] <= 2:
                return "新用户"
            elif row["R_score"] <= 2 and row["F_score"] >= 3 and row["M_score"] >= 3:
                return "流失高价值用户"
            else:
                return "一般用户"

        # apply对每一行数据执行rfm_segment函数
        self.rfm["segment"] = self.rfm.apply(rfm_segment,axis=1)

        # 保存RFM结果
        self.rfm.to_csv("output/reports/rfm_results.csv",index=True)
        print("RFM分析结果已保存")

    def visualize_rfm(self):
        """可视化RFM分析结果"""
        if self.rfm is None or "segment" not in self.rfm.columns:
            print("请先完成用户分群")
            return

        # 1. 用户分群占比饼图
        segment_counts = self.rfm["segment"].value_counts()

        plt.figure(figsize=(10,6))
        plt.pie(segment_counts,labels = segment_counts.index,
                autopct="%1.1f%%",
                startangle = 178,
                )
        plt.title("用户分群占比")
        plt.tight_layout()
        plt.savefig("output/charts/rfm_segment_comparision.png",dpi=300)
        plt.close()
        print("用户分群占比饼图已保存")

        # 2. 各分群RFM指标对比
        segment_rfm = self.rfm.groupby("segment")[["R","F","M"]].mean()

        plt.figure(figsize=(14,6))
        ax = segment_rfm.plot(kind="bar",ax = plt.gca(),width=0.4)
        plt.xticks(rotation=0)
        plt.title("各用户分群RFM指标平均值",fontsize=14)
        for p in ax.patches: # 遍历每条图形
            height = p.get_height()
            ax.annotate(
                f"{height:.2f}",
                (p.get_x() + p.get_width() / 2.,height), # 标签位置
                ha = "center",
                va = "bottom",
                fontsize = 10
            )
        plt.xlabel("用户分群",fontsize=14)
        plt.ylabel("指标值",fontsize=14)
        plt.legend(title="RFM指标")
        plt.tight_layout()
        plt.savefig("output/charts/rfm_segment_comparision.png",dpi=300)
        plt.close()
        print("用户分群RFM指标对比图已保存")

        # 3.RFM得分分布热力图
        # 按用户分群计算平均得分
        segment_scores = self.rfm.groupby("segment")[['R_score',
        'F_score', 'M_score']].mean()
        plt.figure(figsize=(12,6))
        sns.heatmap(segment_scores,annot=True,cmap="coolwarm")
        plt.title("各用户分群RFM平均得分热力图")
        plt.tight_layout()
        plt.ylabel("平均得分")
        plt.savefig("output/charts/rfm_heatmap.png",dpi=300)
        plt.close()
        print("RFM得分热力图已保存")

        # 输出各分群数量和占比
        segment_stats = pd.DataFrame({
            "用户数量":self.rfm["segment"].value_counts(),
            "占比":self.rfm["segment"].value_counts(normalize=True).map(lambda x:f"{x:.2%}")
        })
        segment_stats.to_csv("output/reports/rfm_user_num.csv")

        print("\n用户分群统计：")
        print(segment_stats)



# 在main.py中调用
if __name__ == "__main__":
    # 假设已经加载并清洗了数据
    cleaned_df = pd.read_csv('data/processed/cleaned_user_behaviors.csv')
    # 转换日期列
    cleaned_df['date'] = pd.to_datetime(cleaned_df['date'])
    
    rfm_analyzer = RFMAnalyzer(cleaned_df)
    rfm_analyzer.calculate_rfm()
    rfm_analyzer.score_rfm()
    rfm_analyzer.segment_users()
    rfm_analyzer.visualize_rfm()
### 3.6 主程序入口
创建`main.py`文件，作为项目执行入口：
from src.data_exploration import DataExplorer
from src.data_clean import DataCleaner
from src.eda import EDAnalyzer
from src.rfm_analysis import RFMAnalyzer

def main():
    """项目主函数"""
    print("===== 电商销售数据分析项目 =====")

    # 1.数据读取和初步探索
    print("\n===== 阶段1：数据探索 =====")
    explorer = DataExplorer("data/raw/UserBehavior.csv")
    if not explorer.read_data():
        print("数据读取失败，项目终止")
        return
    raw_df = explorer.basic_exploration()

    # 2. 数据清洗
    print("\n===== 阶段2：数据清洗 =====")
    cleaner = DataCleaner(raw_df)
    cleaned_df = cleaner.clean()

    # 3. 探索性数据分析
    print("\n===== 阶段3：探索性数据分析 =====")
    eda_analyzer = EDAnalyzer(cleaned_df)
    # 创建输出目录
    eda_analyzer._create_output_dirs()
    # 整体数据分析
    eda_analyzer.overall_analysis()
    # 时间维度分析
    eda_analyzer.time_based_anlysis()
    # 商品维度分析
    eda_analyzer.product_based_analysis()
    # 用户行为路径分析
    eda_analyzer.user_behavior_path_analysis()

    # 4. RFM分析
    print("\n===== 阶段4：RFM用户价值分析 =====")
    rfm = RFMAnalyzer(cleaned_df)
    # 计算RFM指标
    rfm.calculate_rfm()
    # 为RFM指标打分
    rfm.score_rfm()
    # 用户分群
    rfm.segment_users()
    # 可视化RFM分析结果
    rfm.visualize_rfm()

    print("\n===== 数据分析完成 =====")
    print("分析结果已保存至output目录下")


if __name__ == "__main__":
    main()

## 四、项目成果与可视化报告

### 4.1 主要结论
1. 用户行为规律：
   - 每日活跃高峰出现在13:00-14:00，此时段购买转化率最高
   - 周末用户活跃度明显高于工作日，尤其是周六
   - 整体转化漏斗显示，点击到购买的转化率约为1.18%，加购到购买的流失率较高

2. 商品分析：
   - 商品ID为2124040和2964774的商品购买次数最多，具有市场优势
   - 商品品类4756105的购买量远远大于其他商品品类，具有市场竞争优势

3. 用户分群：
   - 高价值用户占比约12.30%，需要制定有效的留存策略
   - 流失高价值用户占比约3.34%，近期未购买但历史价值高，有召回潜力
   - 新用户占比约40.27%，占比量大，需要制定有效的留存策略

### 4.2 业务建议
1. 营销活动优化：
   - 在13:00-14:00及周末开展促销活动，提高转化率
   - 针对高价值用户推出会员专属福利和提前购权益，提升忠诚度
   - 对流失高价值用户发送定向优惠券和个性化推荐，刺激回归消费

2. 商品策略：
   - 增加高转化率品类的库存和曝光，考虑扩展相关产品线
   - 对低转化率品类进行优化或调整，分析原因（价格、评价、详情页等）
   - 根据用户偏好优化商品推荐算法，提高精准度

3. 用户运营：
   - 为新用户提供新手引导和专属优惠，提高首次购买率
   - 针对潜力用户开展复购激励活动，如满减券、积分翻倍，回归签到奖励等
   - 优化购物流程，减少从加购到购买的流失，如简化结算步骤、提供快速配送

### 4.3 报告呈现
将分析结果整理为包含以下部分的报告：
- 项目背景与目标
- 数据说明与处理过程
- 核心发现（结合可视化图表）
- 业务建议

可使用以下方式呈现：
1. Python生成的图表和文本报告（已保存在output目录）
2. Excel汇总分析结果（可使用透视表和图表）
3. Tableau制作交互式仪表盘
4. PPT演示文稿（用于汇报）


