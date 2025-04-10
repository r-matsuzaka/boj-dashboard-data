import pandas as pd
import os
import re

def extract_and_save_tl_data(excel_file, output_csv=None):
    """
    毎月勤労統計調査Excelファイルから年平均データを抽出し、CSVとして保存する
    
    Args:
        excel_file (str): 毎月勤労統計調査のExcelファイルパス
        output_csv (str, optional): 出力CSVファイルパス
    
    Returns:
        str: 保存したCSVファイルのパス
    """
    if output_csv is None:
        # 出力ファイル名の設定（デフォルト）
        dir_name = os.path.dirname(excel_file)
        base_name = os.path.splitext(os.path.basename(excel_file))[0]
        output_csv = os.path.join(dir_name, f"{base_name}_年平均.csv")
    
    print(f"Excelファイルを読み込んでいます: {excel_file}")
    
    try:
        # TLシートを読み込む
        xl = pd.ExcelFile(excel_file)
        
        # シート名の確認
        sheet_names = xl.sheet_names
        print(f"シート名一覧: {sheet_names}")
        
        # TLシートがある場合はそれを使用、なければ最初のシートを使用
        sheet_name = 'TL' if 'TL' in sheet_names else sheet_names[0]
        print(f"使用するシート: {sheet_name}")
        
        # シートを読み込む（ヘッダーなしで読み込み）
        df = xl.parse(sheet_name, header=None)
        
        # データの構造を確認
        print(f"データ形状: {df.shape}")
        
        # 年データを含む列（A列）と年平均データ（B列）を特定する
        year_col = 0  # A列は0番目
        value_col = 1  # B列は1番目
        
        # 指数と前年比のセクションを分ける文字列があるか探す
        section_row = None
        for i, row in df.iterrows():
            # "前年比" または "Year-on-year growth rates" などのテキストを探す
            cell_value = str(row.iloc[0]).lower() if not pd.isna(row.iloc[0]) else ""
            if "前年比" in cell_value or "year-on-year" in cell_value:
                section_row = i
                print(f"前年比セクションの開始行: {section_row}")
                break
        
        if section_row is None:
            # セクション区切りが見つからない場合は、データの特性から推測する
            # 例: 近年のデータは後半にあることが多いので、データセットの半分で分ける
            section_row = df.shape[0] // 2
            print(f"前年比セクションの区切りが見つからなかったため、推定: {section_row}")
        
        # データを抽出する
        indices_data = []  # 指数データ
        growth_data = []   # 前年比データ
        
        # 指数データを抽出
        for i in range(0, section_row):
            row = df.iloc[i]
            # 年と数値のペアを探す
            year_value = row.iloc[year_col]
            value = row.iloc[value_col]
            
            # 年と思われる値かどうかを確認（4桁の数字）
            if isinstance(year_value, (int, float)) and 1900 <= year_value <= 2100:
                # 数値かどうかを確認
                if isinstance(value, (int, float)) and not pd.isna(value):
                    indices_data.append((int(year_value), float(value)))
        
        # 前年比データを抽出
        for i in range(section_row + 1, df.shape[0]):
            row = df.iloc[i]
            year_value = row.iloc[year_col]
            value = row.iloc[value_col]
            
            if isinstance(year_value, (int, float)) and 1900 <= year_value <= 2100:
                if isinstance(value, (int, float)) and not pd.isna(value):
                    growth_data.append((int(year_value), float(value)))
        
        print(f"抽出した指数データ数: {len(indices_data)}")
        print(f"抽出した前年比データ数: {len(growth_data)}")
        
        # データをマージする
        merged_data = []
        all_years = sorted(set([year for year, _ in indices_data] + [year for year, _ in growth_data]))
        
        for year in all_years:
            # 指数データから該当年のデータを探す
            index_value = next((value for y, value in indices_data if y == year), None)
            # 前年比データから該当年のデータを探す
            growth_value = next((value for y, value in growth_data if y == year), None)
            
            # 両方または片方のデータが存在する場合に追加
            if index_value is not None or growth_value is not None:
                merged_data.append({
                    '年': year,
                    '指数': index_value,
                    '前年比': growth_value
                })
        
        # DataFrameに変換
        result_df = pd.DataFrame(merged_data)
        
        # CSVに保存
        result_df.to_csv(output_csv, index=False, encoding='utf-8')
        print(f"データをCSVに保存しました: {output_csv}")
        
        return output_csv
        
    except Exception as e:
        print(f"データ抽出中にエラーが発生しました: {e}")
        return None

# メイン実行部分
if __name__ == "__main__":
    # ダウンロードしたExcelファイルのパス
    excel_file = os.path.join(os.getcwd(), "data", "毎月勤労統計調査.xlsx")
    
    # データ抽出とCSV保存を実行
    csv_file = extract_and_save_tl_data(excel_file)
    
    if csv_file:
        print(f"年平均データを正常に抽出しました: {csv_file}")
    else:
        print("データの抽出に失敗しました")