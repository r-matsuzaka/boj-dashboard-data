#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
process_di.py - 内閣府ESRIのExcelファイルまたはテキストデータからCI指数とDI指数のデータを抽出して
CSVとして保存するスクリプト
"""

import os
import pandas as pd
import pathlib
import re
import numpy as np

def main():
    print("CI指数とDI指数のデータ処理を開始します...")
    
    # dataディレクトリを確認
    data_dir = pathlib.Path('data')
    data_dir.mkdir(exist_ok=True)
    
    # まずpaste.txtファイルを確認（優先度高）
    documents_dir = pathlib.Path('documents')
    paste_file = documents_dir / 'paste.txt'
    if documents_dir.exists() and paste_file.exists():
        print("paste.txtファイルからデータを処理します...")
        try:
            if process_paste_file(paste_file, data_dir):
                return
        except Exception as e:
            print(f"paste.txtからの処理に失敗しました: {e}")
    
    # Excelファイルを探す
    excel_files = list(data_dir.glob("*CI*DI*.xls*"))
    if not excel_files:
        print("dataディレクトリにCI/DI指数のExcelファイルが見つかりません。")
        return
    
    # 最新のファイルを使用
    excel_file = excel_files[0]
    print(f"処理するファイル: {excel_file}")
    
    try:
        process_excel_file(excel_file, data_dir)
    except Exception as e:
        print(f"Excelファイルの処理に失敗しました: {e}")

def process_paste_file(paste_file, data_dir):
    """paste.txtファイルを処理する"""
    with open(paste_file, 'r', encoding='utf-8') as f:
        paste_content = f.read()
    
    # タブ区切りテキストとして処理
    lines = paste_content.strip().split('\n')
    
    # ヘッダー行を探す
    header_line = None
    for i, line in enumerate(lines):
        if "先行指数" in line and "一致指数" in line and "遅行指数" in line:
            header_line = i
            break
    
    if header_line is None:
        print("ヘッダー行が見つかりませんでした。")
        return False
    
    # 時間列を特定
    time_col = None
    year_col = None
    month_col = None
    for i in range(header_line):
        cells = lines[i].split('\t')
        for j, cell in enumerate(cells):
            if "時間軸コード" in cell or "Time" in cell:
                time_col = j
            elif "西暦年" in cell or "Calendar" in cell:
                year_col = j
            elif "月" in cell or "Month" in cell:
                month_col = j
    
    if year_col is None or month_col is None:
        print("年・月の列が見つかりませんでした。")
        return False
    
    # 固定列インデックスを使用（スクリーンショットに基づく）
    # CI指数: D, E, F列 (インデックス 3, 4, 5)
    # DI指数: J, K, L列 (インデックス 9, 10, 11)
    ci_leading_idx = 3  # D列
    ci_coincident_idx = 4  # E列
    ci_lagging_idx = 5  # F列
    
    di_leading_idx = 9  # J列
    di_coincident_idx = 10  # K列
    di_lagging_idx = 11  # L列
    
    print(f"使用する列インデックス:")
    print(f"CI先行指数: {ci_leading_idx}, CI一致指数: {ci_coincident_idx}, CI遅行指数: {ci_lagging_idx}")
    print(f"DI先行指数: {di_leading_idx}, DI一致指数: {di_coincident_idx}, DI遅行指数: {di_lagging_idx}")
    
    # データ行の解析（ヘッダー行から3行後がデータ開始）
    data_start = header_line + 3
    data = []
    
    for i in range(data_start, len(lines)):
        line = lines[i].strip()
        if not line:
            continue
        
        cells = line.split('\t')
        if len(cells) <= max(year_col, month_col):
            continue
        
        # 年月を取得
        try:
            year = cells[year_col].strip()
            month = cells[month_col].strip()
            
            # 数値でない場合はスキップ
            if not year or not month or not year.isdigit() or not month.isdigit():
                continue
            
            # yyyymm形式に変換
            yyyymm = f"{int(year)}{int(month):02d}"
            
            # 各指数の値を取得
            row = {'yyyymm': yyyymm}
            
            # CI指数
            if ci_leading_idx < len(cells):
                row['CI_先行指数'] = cells[ci_leading_idx].strip() or None
            else:
                row['CI_先行指数'] = None
                
            if ci_coincident_idx < len(cells):
                row['CI_一致指数'] = cells[ci_coincident_idx].strip() or None
            else:
                row['CI_一致指数'] = None
                
            if ci_lagging_idx < len(cells):
                row['CI_遅行指数'] = cells[ci_lagging_idx].strip() or None
            else:
                row['CI_遅行指数'] = None
            
            # DI指数
            if di_leading_idx < len(cells):
                row['DI_先行指数'] = cells[di_leading_idx].strip() or None
            else:
                row['DI_先行指数'] = None
                
            if di_coincident_idx < len(cells):
                row['DI_一致指数'] = cells[di_coincident_idx].strip() or None
            else:
                row['DI_一致指数'] = None
                
            if di_lagging_idx < len(cells):
                row['DI_遅行指数'] = cells[di_lagging_idx].strip() or None
            else:
                row['DI_遅行指数'] = None
            
            data.append(row)
        except Exception as e:
            print(f"行 {i+1} の処理中にエラーが発生しました: {e}")
            continue
    
    if not data:
        print("有効なデータが見つかりませんでした。")
        return False
    
    # データフレームに変換
    result_df = pd.DataFrame(data)
    
    # 数値型に変換
    for col in result_df.columns:
        if col != 'yyyymm':
            result_df[col] = pd.to_numeric(result_df[col], errors='coerce')
    
    # CSVとして保存
    output_file = data_dir / "景気動向指数.csv"
    result_df.to_csv(output_file, index=False, encoding='utf-8')
    
    print(f"処理が完了しました。データは {output_file} に保存されました。")
    print(f"データ件数: {len(result_df)}行")
    return True

def process_excel_file(excel_file, data_dir):
    """Excelファイルを処理する"""
    # Excelファイルを読み込む
    print("Excelファイルを読み込んでいます...")
    try:
        # シート名を取得
        xl = pd.ExcelFile(excel_file)
        sheet_name = xl.sheet_names[0]  # 最初のシートを使用
        df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
        print(f"シート '{sheet_name}' を読み込みました。")
    except Exception as e:
        print(f"Excelファイルの読み込みに失敗しました: {e}")
        return False
    
    # ヘッダー行を探す
    header_row = None
    for i in range(min(10, len(df))):
        row_values = df.iloc[i].astype(str)
        if row_values.str.contains('先行指数').any() and row_values.str.contains('一致指数').any() and row_values.str.contains('遅行指数').any():
            header_row = i
            break
    
    if header_row is None:
        print("ヘッダー行が見つかりませんでした。")
        return False
    
    # 固定列インデックスを使用（スクリーンショットから判断）
    # CI指数: D, E, F列 (3, 4, 5)
    # DI指数: J, K, L列 (9, 10, 11)
    
    column_indices = {
        'CI_先行指数': 3,  # D列
        'CI_一致指数': 4,  # E列
        'CI_遅行指数': 5,  # F列
        'DI_先行指数': 9,  # J列
        'DI_一致指数': 10, # K列
        'DI_遅行指数': 11, # L列
    }
    
    # 時間軸の列を特定
    time_col = None
    year_col = None
    month_col = None
    
    for j in range(len(df.columns)):
        cell_value = str(df.iloc[header_row, j])
        if "時間軸コード" in cell_value or "Time" in cell_value:
            time_col = j
        elif "西暦年" in cell_value or "Calendar" in cell_value:
            year_col = j
        elif "月" in cell_value or "Month" in cell_value:
            month_col = j
    
    if year_col is None or month_col is None:
        print("年・月の列が見つかりませんでした。")
        return False
    
    # データの開始行を特定（ヘッダー行の2行後から）
    data_start_row = header_row + 2
    
    # データを抽出
    data = []
    for i in range(data_start_row, len(df)):
        # スキップすべき空行かどうかを確認
        if pd.isna(df.iloc[i, year_col]) or pd.isna(df.iloc[i, month_col]):
            continue
        
        # 年月の取得
        year = df.iloc[i, year_col]
        month = df.iloc[i, month_col]
        
        # yyyymm形式に変換
        yyyymm = f"{int(year)}{int(month):02d}"
        
        # 各指数の値を取得
        row_data = {'yyyymm': yyyymm}
        
        # CI指数
        if 'CI_先行指数' in column_indices:
            row_data['CI_先行指数'] = df.iloc[i, column_indices['CI_先行指数']]
        else:
            row_data['CI_先行指数'] = None
            
        if 'CI_一致指数' in column_indices:
            row_data['CI_一致指数'] = df.iloc[i, column_indices['CI_一致指数']]
        else:
            row_data['CI_一致指数'] = None
            
        if 'CI_遅行指数' in column_indices:
            row_data['CI_遅行指数'] = df.iloc[i, column_indices['CI_遅行指数']]
        else:
            row_data['CI_遅行指数'] = None
        
        # DI指数
        if 'DI_先行指数' in column_indices:
            row_data['DI_先行指数'] = df.iloc[i, column_indices['DI_先行指数']]
        else:
            row_data['DI_先行指数'] = None
            
        if 'DI_一致指数' in column_indices:
            row_data['DI_一致指数'] = df.iloc[i, column_indices['DI_一致指数']]
        else:
            row_data['DI_一致指数'] = None
            
        if 'DI_遅行指数' in column_indices:
            row_data['DI_遅行指数'] = df.iloc[i, column_indices['DI_遅行指数']]
        else:
            row_data['DI_遅行指数'] = None
        
        data.append(row_data)
    
    if not data:
        print("有効なデータが見つかりませんでした。")
        return False
    
    # データフレームに変換
    result_df = pd.DataFrame(data)
    
    # 数値型に変換
    for col in result_df.columns:
        if col != 'yyyymm':
            result_df[col] = pd.to_numeric(result_df[col], errors='coerce')
    
    # CSVとして保存
    output_file = data_dir / "景気動向指数.csv"
    result_df.to_csv(output_file, index=False, encoding='utf-8')
    
    print(f"処理が完了しました。データは {output_file} に保存されました。")
    print(f"データ件数: {len(result_df)}行")
    return True

if __name__ == "__main__":
    main()