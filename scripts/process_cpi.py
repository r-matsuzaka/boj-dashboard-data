import pandas as pd
import os
import re
from datetime import datetime

def transform_cpi_csv(input_csv, output_csv):
    # CSVファイルの読み込み
    df = pd.read_csv(input_csv)
    
    print(f"読み込んだCSVの列数: {len(df.columns)}")
    
    # データの前処理
    # 実データの開始行を特定（「時間軸コード」が含まれる行）
    start_row = None
    time_col_index = None
    for i, row in df.iterrows():
        for j, val in enumerate(row):
            if isinstance(val, str) and '時間軸コード' in str(val):
                start_row = i
                time_col_index = j
                break
        if start_row is not None:
            break
    
    if start_row is None:
        print("時間軸コードの列が見つかりませんでした")
        return False
    
    print(f"データ開始行: {start_row}, 時間軸コード列インデックス: {time_col_index}")
    
    # 「総合」列のインデックスを見つける
    total_col_index = None
    for j, val in enumerate(df.iloc[start_row-2]):  # 類・品目の行
        if isinstance(val, str) and '総合' in str(val):
            total_col_index = j
            break
    
    if total_col_index is None:
        print("総合列が見つかりませんでした。12列目を使用します")
        total_col_index = 12  # デフォルト値
    
    print(f"総合列インデックス: {total_col_index}")
    
    # データの抽出
    year_month_data = []
    total_values = []
    
    # 実データの読み取り（時間軸コードの行の次から）
    for i in range(start_row + 1, len(df)):
        row = df.iloc[i]
        
        # 年月の取得
        year_month = None
        for j in range(time_col_index, time_col_index + 3):  # 時間軸コードの周辺列を確認
            val = row.iloc[j] if j < len(row) else None
            if pd.notna(val) and isinstance(val, str) and '年' in val and '月' in val:
                match = re.search(r'(\d{4})年(\d{1,2})月', str(val))
                if match:
                    year = match.group(1)
                    month = match.group(2).zfill(2)
                    year_month = f"{year}/{month}"
                    break
        
        if year_month is None:
            continue
        
        # 総合値の取得
        total_value = row.iloc[total_col_index] if total_col_index < len(row) else None
        
        # 有効な値の場合のみ追加
        if pd.notna(total_value) and str(total_value).strip() != '*' and str(total_value).strip() != '-':
            try:
                # 数値に変換できることを確認
                value = float(str(total_value).replace(',', ''))
                year_month_data.append(year_month)
                total_values.append(value)
            except ValueError:
                print(f"数値変換エラー: {total_value} - 行: {i}")
    
    # 結果のDataFrameを作成
    result_df = pd.DataFrame({
        '年月': year_month_data,
        '前年同月比': total_values
    })
    
    # CSVに保存
    result_df.to_csv(output_csv, index=False, encoding='utf-8')
    print(f"変換完了: {output_csv}")
    print(f"抽出したデータ数: {len(result_df)}")
    
    # 最初と最後の行を表示
    if len(result_df) > 0:
        print("\n最初の5行:")
        print(result_df.head(5).to_string())
        print("\n最後の5行:")
        print(result_df.tail(5).to_string())
    
    return True

if __name__ == "__main__":
    # 単一ファイルの処理
    input_csv = "cpi_data/CPI_2025_02_中分類指数_全国_月次_前年同月比.csv"
    output_csv = "cpi_data/CPI_2025_02_総合_前年同月比.csv"
    
    # ファイルが存在するか確認
    if os.path.exists(input_csv):
        transform_cpi_csv(input_csv, output_csv)
    else:
        print(f"ファイルが見つかりません: {input_csv}")