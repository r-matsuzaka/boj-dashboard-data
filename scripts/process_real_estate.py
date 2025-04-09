import pandas as pd
import numpy as np
from pathlib import Path
import os
from datetime import datetime

def process_real_estate_data():
    """
    Excelファイルから東京都の商業用不動産価格指数データを抽出し、CSVとして保存します。
    日付形式のデータを年に変換して処理します。
    """
    # プロジェクトのルートディレクトリとデータパスを取得
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    data_dir = project_root / "data"
    
    # 入力Excelファイルのパス
    input_file = data_dir / "commercial_real_estate_price_index.xlsx"
    output_file = data_dir / "tokyo_commercial_real_estate_price_index.csv"
    
    if not os.path.exists(input_file):
        print(f"エラー: 入力ファイルが見つかりません: {input_file}")
        return False
    
    try:
        # 東京都のシートを指定
        tokyo_sheet = "東京都Tokyo"
        print(f"Excelファイル読み込み中: {input_file}, シート: {tokyo_sheet}")
        
        # ヘッダーなしでデータを読み込む
        raw_df = pd.read_excel(input_file, sheet_name=tokyo_sheet, header=None)
        print(f"シートの寸法: {raw_df.shape[0]}行 × {raw_df.shape[1]}列")
        
        # 画像と確認した内容から特定した情報を直接使用
        jp_header_row = 5  # 日本語ヘッダー行
        price_index_row = 8  # 価格指数行
        data_start_row = 11  # データ開始行
        
        # 日本語ヘッダー行の内容を表示
        print(f"\n日本語ヘッダー行（{jp_header_row}行目）の内容:")
        header_values = [str(val) for val in raw_df.iloc[jp_header_row] if pd.notna(val)]
        print(", ".join(header_values[:10]))
        
        # 不動産タイプの列インデックスを画像と確認した内容から直接設定
        # 価格指数の列インデックスを特定（各不動産タイプの最初の列）
        property_indices = {
            "商業用不動産総合": 2,  # 商業用不動産総合の価格指数列
            "店舗": 8,            # 店舗の価格指数列
            "オフィス": 11,        # オフィスの価格指数列
            "倉庫": 14,           # 倉庫の価格指数列
            "工場": 17,           # 工場の価格指数列
            "ﾏﾝｼｮﾝ･ｱﾊﾟｰﾄ": 20      # マンション・アパートの価格指数列
        }
        
        # 結果データフレームを作成
        result_df = pd.DataFrame()
        
        # 1列目から日付を抽出し、年に変換
        years = []
        for row in range(data_start_row, raw_df.shape[0]):
            date_value = raw_df.iloc[row, 0]  # 1列目（0インデックス）の日付
            
            if pd.isna(date_value):
                break  # 空白行に到達したら終了
                
            # 日付から年を抽出
            try:
                if isinstance(date_value, datetime):
                    # datetime型の場合
                    year = date_value.year
                    years.append(year)
                elif isinstance(date_value, str):
                    # 文字列の場合、日付としてパース
                    if '-' in date_value:
                        year = int(date_value.split('-')[0])
                        years.append(year)
                    else:
                        # 数字のみの文字列の場合
                        try:
                            year = int(float(date_value))
                            if 1980 <= year <= 2030:
                                years.append(year)
                        except ValueError:
                            break
                elif isinstance(date_value, (int, float)):
                    # 数値の場合
                    year = int(date_value)
                    if 1980 <= year <= 2030:
                        years.append(year)
                    else:
                        break
                else:
                    print(f"未知の日付形式: {date_value}, タイプ: {type(date_value)}")
                    break
            except Exception as e:
                print(f"日付解析エラー: {date_value}, エラー: {e}")
                break
        
        if not years:
            print("エラー: 年データを抽出できませんでした。データの最初の数行を確認:")
            for row in range(data_start_row, data_start_row + 5):
                if row < raw_df.shape[0]:
                    print(f"行 {row}: {[raw_df.iloc[row, col] for col in range(min(5, raw_df.shape[1]))]}")
            return False
        
        result_df["Year"] = years
        print(f"{len(years)}年分のデータを抽出しました: {min(years)}年から{max(years)}年まで")
        
        # 各不動産タイプの価格指数を追加
        property_mapping = {
            "商業用不動産総合": "Commercial_Property",
            "店舗": "Retail",
            "オフィス": "Office", 
            "倉庫": "Warehouse",
            "工場": "Factory",
            "ﾏﾝｼｮﾝ･ｱﾊﾟｰﾄ": "Apartment"
        }
        
        # デバッグのために各不動産タイプの最初の値を表示
        print("\n各不動産タイプの最初の値（データ開始行）:")
        for jp_type, col_idx in property_indices.items():
            if col_idx < raw_df.shape[1]:
                value = raw_df.iloc[data_start_row, col_idx]
                print(f"{jp_type}: {value}")
        
        for jp_type, en_type in property_mapping.items():
            col_idx = property_indices.get(jp_type)
            if col_idx is not None and col_idx < raw_df.shape[1]:
                # データを抽出
                values = []
                for i, row in enumerate(range(data_start_row, data_start_row + len(years))):
                    if row < raw_df.shape[0]:
                        cell_value = raw_df.iloc[row, col_idx]
                        if pd.notna(cell_value) and isinstance(cell_value, (int, float)):
                            values.append(float(cell_value))
                        else:
                            values.append(np.nan)
                    else:
                        values.append(np.nan)
                
                if len(values) == len(years):
                    result_df[en_type] = values
                    print(f"{en_type}の価格指数を追加しました。最初の値: {values[0] if values else 'N/A'}")
                else:
                    print(f"警告: {jp_type}のデータサイズが年の数と一致しません。スキップします。")
        
        # CSVとして保存
        result_df.to_csv(output_file, index=False)
        print(f"\n東京都の商業用不動産価格指数データを保存しました: {output_file}")
        print(f"データには{len(result_df)}年分の以下の不動産タイプが含まれています:")
        for col in result_df.columns:
            if col != "Year":
                print(f"  - {col}")
        
        print("\nデータのプレビュー:")
        print(result_df.head())
        
        return True
    
    except Exception as e:
        print(f"Excelファイルの処理中にエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    process_real_estate_data()