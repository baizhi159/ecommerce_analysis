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