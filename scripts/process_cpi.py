import pandas as pd
import os
import re
from datetime import datetime, timedelta

def transform_cpi_csv(input_csv, output_csv, data_type="前年同月比"):
    """
    CPIデータを変換するメイン関数
    
    Args:
        input_csv: 入力CSVファイルパス
        output_csv: 出力CSVファイルパス
        data_type: データタイプ（"前年同月比"または"指数"）
    """
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
    column_name = "前年同月比" if data_type == "前年同月比" else "指数"
    result_df = pd.DataFrame({
        '年月': year_month_data,
        column_name: total_values
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

def merge_cpi_data(yoy_csv, index_csv, output_csv):
    """
    前年同月比データと指数データを結合する
    
    Args:
        yoy_csv: 前年同月比CSVファイルパス
        index_csv: 指数CSVファイルパス
        output_csv: 出力CSVファイルパス
    """
    try:
        # 前年同月比データ読み込み
        yoy_df = pd.read_csv(yoy_csv)
        
        # 指数データ読み込み
        index_df = pd.read_csv(index_csv)
        
        # データを年月で結合
        merged_df = pd.merge(yoy_df, index_df, on='年月', how='outer')
        
        # 欠損値を確認
        missing_count = merged_df.isnull().sum().sum()
        if missing_count > 0:
            print(f"結合後のデータに {missing_count} 個の欠損値があります")
            
        # 結合データを保存
        merged_df.to_csv(output_csv, index=False, encoding='utf-8')
        print(f"結合完了: {output_csv}")
        print(f"結合データ数: {len(merged_df)}")
        
        # 最初と最後の行を表示
        print("\n最初の5行:")
        print(merged_df.head(5).to_string())
        print("\n最後の5行:")
        print(merged_df.tail(5).to_string())
        
        return True
    except Exception as e:
        print(f"データ結合中にエラーが発生しました: {e}")
        return False

if __name__ == "__main__":
    # プロジェクトのルートディレクトリへのパスを設定
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    data_dir = os.path.join(project_root, "data")
    
    # dataディレクトリが存在しない場合は作成
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    # 現在の日付から2か月前の年月を取得
    today = datetime.now()
    first_day_of_month = today.replace(day=1)
    # 1か月前
    one_month_ago = first_day_of_month - timedelta(days=1)
    first_day_one_month_ago = one_month_ago.replace(day=1)
    # 2か月前
    two_months_ago = first_day_one_month_ago - timedelta(days=1)
    
    year = two_months_ago.year
    month = two_months_ago.month
    
    # 前年同月比データの処理
    input_yoy_csv = os.path.join(data_dir, f"CPI_中分類指数_全国_月次_前年同月比.csv")
    output_yoy_csv = os.path.join(data_dir, f"CPI_総合_前年同月比.csv")
    
    print(f"前年同月比データ処理開始:")
    print(f"処理対象ファイル: {input_yoy_csv}")
    print(f"出力ファイル: {output_yoy_csv}")
    
    yoy_success = False
    if os.path.exists(input_yoy_csv):
        yoy_success = transform_cpi_csv(input_yoy_csv, output_yoy_csv, "前年同月比")
    else:
        print(f"ファイルが見つかりません: {input_yoy_csv}")
        
        # ファイルが見つからない場合の代替処理
        possible_dirs = [script_dir, os.path.join(script_dir, "cpi_data"), data_dir]
        
        for dir_path in possible_dirs:
            if os.path.exists(dir_path):
                cpi_files = [f for f in os.listdir(dir_path) if f.startswith("CPI_") and "_中分類指数_全国_月次_前年同月比.csv" in f]
                if cpi_files:
                    print(f"\n{dir_path}内に以下のファイルが見つかりました:")
                    for f in cpi_files:
                        print(f"- {f}")
                    
                    # 最新のファイルを処理
                    latest_file = sorted(cpi_files)[-1]
                    input_yoy_csv = os.path.join(dir_path, latest_file)
                    
                    # 出力ファイル名を調整
                    match = re.search(r'CPI_(\d{4})_(\d{2})_', latest_file)
                    if match:
                        year = match.group(1)
                        month = match.group(2)
                        output_yoy_csv = os.path.join(data_dir, f"CPI_{year}_{month}_総合_前年同月比.csv")
                        
                        print(f"\n代わりに最新ファイルを処理します: {input_yoy_csv}")
                        print(f"出力ファイル: {output_yoy_csv}")
                        yoy_success = transform_cpi_csv(input_yoy_csv, output_yoy_csv, "前年同月比")
                        break
    
    # 指数データの処理
    input_index_csv = os.path.join(data_dir, f"CPI_中分類指数_全国_月次_指数.csv")
    output_index_csv = os.path.join(data_dir, f"CPI_総合_指数.csv")
    
    print(f"\n指数データ処理開始:")
    print(f"処理対象ファイル: {input_index_csv}")
    print(f"出力ファイル: {output_index_csv}")
    
    index_success = False
    if os.path.exists(input_index_csv):
        index_success = transform_cpi_csv(input_index_csv, output_index_csv, "指数")
    else:
        print(f"ファイルが見つかりません: {input_index_csv}")
        
        # ファイルが見つからない場合の代替処理
        possible_dirs = [script_dir, os.path.join(script_dir, "cpi_data"), data_dir]
        
        for dir_path in possible_dirs:
            if os.path.exists(dir_path):
                cpi_files = [f for f in os.listdir(dir_path) if f.startswith("CPI_") and "_中分類指数_全国_月次_指数.csv" in f]
                if cpi_files:
                    print(f"\n{dir_path}内に以下のファイルが見つかりました:")
                    for f in cpi_files:
                        print(f"- {f}")
                    
                    # 最新のファイルを処理
                    latest_file = sorted(cpi_files)[-1]
                    input_index_csv = os.path.join(dir_path, latest_file)
                    
                    # 出力ファイル名を調整
                    match = re.search(r'CPI_(\d{4})_(\d{2})_', latest_file)
                    if match:
                        year = match.group(1)
                        month = match.group(2)
                        output_index_csv = os.path.join(data_dir, f"CPI_{year}_{month}_総合_指数.csv")
                        
                        print(f"\n代わりに最新ファイルを処理します: {input_index_csv}")
                        print(f"出力ファイル: {output_index_csv}")
                        index_success = transform_cpi_csv(input_index_csv, output_index_csv, "指数")
                        break
    
    # 両方のデータが処理できた場合は結合
    if yoy_success and index_success:
        output_merged_csv = os.path.join(data_dir, f"CPI_総合_統合.csv")
        print(f"\n前年同月比と指数のデータを結合します")
        print(f"出力ファイル: {output_merged_csv}")
        merge_cpi_data(output_yoy_csv, output_index_csv, output_merged_csv)
    else:
        print("\n前年同月比と指数の両方のデータが揃っていないため、結合処理はスキップされました")