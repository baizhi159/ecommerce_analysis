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