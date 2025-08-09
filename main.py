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