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