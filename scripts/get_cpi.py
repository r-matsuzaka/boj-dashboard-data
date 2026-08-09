import requests
from bs4 import BeautifulSoup
import os
import re
import pandas as pd
from datetime import datetime, timedelta
import time
import urllib.parse
import sys

# ベースURL
base_url = 'https://www.e-stat.go.jp'

# プロジェクトのルートディレクトリとデータディレクトリの設定
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
data_dir = os.path.join(project_root, "data")

# データディレクトリが存在しない場合は作成
os.makedirs(data_dir, exist_ok=True)

# 現在の年月から2か月前を計算する関数
def get_two_months_ago():
    today = datetime.now()
    first_day_of_month = today.replace(day=1)
    # 1か月前
    one_month_ago = first_day_of_month - timedelta(days=1)
    first_day_one_month_ago = one_month_ago.replace(day=1)
    # 2か月前
    two_months_ago = first_day_one_month_ago - timedelta(days=1)
    
    return two_months_ago.year, two_months_ago.month

# e-Statの月コード生成関数
def generate_month_code(month):
    """e-Statのmonthパラメータを生成する。

    形式は {半期}{四半期}{四半期の開始月}{四半期の終了月}{対象月} の8桁。
      1月 -> 11010301   4月 -> 12040604   7月 -> 23070907   10月 -> 24101210
      3月 -> 11010303   6月 -> 12040606   9月 -> 23070909   12月 -> 24101212

    四半期のプレフィックスを 110103 に決め打ちすると1〜3月しか正しい値にならず、
    4〜12月は存在しない月コードになって検索結果0件が返る。
    """
    if not 1 <= month <= 12:
        raise ValueError(f"月の値が不正です: {month}")

    quarter = (month - 1) // 3 + 1
    half = (quarter - 1) // 2 + 1
    start_month = (quarter - 1) * 3 + 1
    end_month = quarter * 3

    return f'{half}{quarter}{start_month:02d}{end_month:02d}{month:02d}'

# 特定の年月のURL生成関数
def generate_url(year, month):
    month_code = generate_month_code(month)
    return f'https://www.e-stat.go.jp/stat-search/files?page=1&layout=datalist&toukei=00200573&tstat=000001150147&cycle=1&year={year}0&month={month_code}&tclass1=000001150149&result_back=1&tclass2val=0'

# ファイルをダウンロードする関数
def download_file(url, filename):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    file_path = os.path.join(data_dir, filename)
    with open(file_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    return file_path

# e-Statの統計表一覧のDOM構造
#
# 一覧は<table>/<tr>ではなく、article + ul/li で組まれた擬似テーブル。
# 1つのarticleが1レコードで、2つのliを持つ。
#
#   div.stat-dataset_list
#     article.stat-dataset_list-item          ← 1レコード
#       ul.stat-dataset_list-detail
#         li.stat-dataset_list-detail-item    ← [0] 表番号セル
#           span.stat-sp "表番号"             （スマホ用ラベル）
#           span         "1-1"                （class無し = 表番号本体）
#         li.stat-dataset_list-detail-item    ← [1] 本体行
#           span         "中分類指数（全国）"
#           a.js-data    "月次"               （2階層の表では周期）
#           span.stat-sp "調査年月" + "2026年6月"
#           a.js-dl.stat-icon_0               ← EXCEL本体 (fileKind=0)
#           a.js-dl.stat-icon_4               ← EXCEL閲覧用 (fileKind=4)
#           a.stat-database_icon              ← DB（js-dlを持たない）
#
# 同じ表番号でも 月次 / 年平均 / 年度平均 は別のarticleに分かれる。
DATASET_ITEM_SELECTOR = 'article.stat-dataset_list-item'
DETAIL_ITEM_SELECTOR = 'li.stat-dataset_list-detail-item'

def _text_of(element):
    return element.get_text(strip=True) if element is not None else ''

def extract_table_number(article):
    """articleの1つ目のliから表番号（例: '1-1'）を取り出す。

    スマホ用ラベル <span class="stat-sp">表番号</span> が先行するため、
    class無しのspanだけを見る。
    """
    detail_items = article.select(DETAIL_ITEM_SELECTOR)
    if not detail_items:
        return ''

    for span in detail_items[0].find_all('span'):
        if 'stat-sp' in (span.get('class') or []):
            continue
        text = _text_of(span)
        if text:
            return text
    return ''

def has_cycle(article, cycle):
    """周期（月次 / 年平均 / 年度平均）が完全一致で含まれるかを判定する。

    周期テキストの置き場所は表の階層の深さで変わる。
      2階層の表（1-1など）: a.js-data のテキストが周期そのもの
      3階層の表（4-1など）: class無しのspanが周期で、a.js-dataは末端の品目名
    そのため要素の種類を限定せず、完全一致で探す。

    部分一致にすると「年平均」が「年度平均」に引っかかる危険があるため、
    必ず完全一致で比較する。
    """
    for element in article.find_all(['a', 'span']):
        if _text_of(element) == cycle:
            return True
    return False

def find_excel_link(article):
    """EXCEL本体（fileKind=0）のダウンロードリンクを返す。

    同じ行に「EXCEL閲覧用」(stat-icon_4) と「DB」(stat-database_icon) が
    並ぶため、EXCEL種別(stat-icon_0)とダウンロード(js-dl)の2クラスで絞る。
    クラスの完全一致ではないので、クラスの増減や順序変更に影響されない。
    """
    return article.select_one('a.js-dl.stat-icon_0')

def dump_page_structure(soup, limit=15):
    """抽出に失敗したときに一覧の構造をログへ出す。

    構造が変わった際、ログだけで原因を特定できるようにするための診断。
    これが無いと「ダウンロードリンクが見つかりませんでした」の一行しか
    残らず、ブラウザでDOMを見に行くまで原因が分からない。
    """
    print("--- 一覧の構造ダンプ（診断用） ---")
    articles = soup.select(DATASET_ITEM_SELECTOR)
    print(f"{DATASET_ITEM_SELECTOR}: {len(articles)}件")
    print(f"a.js-dl: {len(soup.select('a.js-dl'))}件 / "
          f"a.js-dl.stat-icon_0: {len(soup.select('a.js-dl.stat-icon_0'))}件")
    print(f"<tr>: {len(soup.find_all('tr'))}個（一覧は擬似テーブルのため0でも異常ではない）")

    for article in articles[:limit]:
        cycles = [c for c in ('月次', '年平均', '年度平均') if has_cycle(article, c)]
        link = find_excel_link(article)
        print(f"  表番号={extract_table_number(article)!r} 周期={cycles} "
              f"EXCEL={link.get('href') if link else None}")
    if len(articles) > limit:
        print(f"  ... 他 {len(articles) - limit} 件")

# ダウンロードしたExcelが目的の年月の月次データか検証する関数
def verify_excel_contents(excel_file, year, month):
    """ブックのどこかに「{year}年{month}月」が含まれることを確認する。

    e-Statのリンク構造が変わって別の統計表を掴んだ場合、ここで弾いて
    既存のCSVを上書きさせない。
    """
    expected = f'{year}年{month}月'
    try:
        xl = pd.ExcelFile(excel_file, engine='openpyxl')
    except Exception as e:
        print(f"Excelファイルを開けませんでした: {e}")
        return False

    print(f"検証: ブック内に「{expected}」が存在するか確認します")
    print(f"シート名一覧: {xl.sheet_names}")

    for sheet_name in xl.sheet_names:
        try:
            df = xl.parse(sheet_name, header=None)
        except Exception as e:
            print(f"シート '{sheet_name}' の読み込みに失敗: {e}")
            continue

        if df.astype(str).apply(lambda col: col.str.contains(expected, regex=False)).any().any():
            print(f"検証OK: シート '{sheet_name}' に「{expected}」を確認しました")
            return True

    print(f"検証NG: ブック内に「{expected}」が見つかりませんでした。")
    print(f"意図した統計表と異なるファイルの可能性があるため、CSVは更新しません。")
    return False

# 複数のシートをそれぞれCSVに変換する関数
def convert_excel_to_csv(excel_file, base_name):
    try:
        # Excelファイルを読み込む
        print(f"Excelファイルを読み込んでいます: {excel_file}")
        
        # 各シートを読み込んでCSVに変換
        xl = pd.ExcelFile(excel_file, engine='openpyxl')
        sheet_names = xl.sheet_names
        print(f"シート名一覧: {sheet_names}")
        
        csv_files = []
        
        for i, sheet_name in enumerate(sheet_names):
            print(f"シート '{sheet_name}' を処理中...")
            
            # シート読み込み
            df = xl.parse(sheet_name)
            
            # CSVファイル名を作成
            if len(sheet_names) > 1:
                sheet_suffix = f"_sheet{i+1}"
                if i == 0:
                    sheet_suffix = "_指数"  # 1枚目のシート（指数）
                elif i == 1:
                    sheet_suffix = "_前月比"  # 2枚目のシート（前月比）
                elif i == 2: 
                    sheet_suffix = "_前年同月比"  # 3枚目のシート（前年同月比）
            else:
                sheet_suffix = ""
                
            csv_filename = f"{base_name}{sheet_suffix}.csv"
            csv_path = os.path.join(data_dir, csv_filename)
            
            # CSVに変換して保存
            df.to_csv(csv_path, index=False, encoding='utf-8')
            print(f"CSVに変換しました: {csv_path}")
            
            csv_files.append(csv_path)
        
        return csv_files
        
    except Exception as e:
        print(f"Excel→CSV変換中にエラーが発生しました: {e}")
        return []

# メイン関数
def download_cpi_data(year=None, month=None):
    # 年月が指定されていない場合は2か月前を使用
    if year is None or month is None:
        year, month = get_two_months_ago()
    
    print(f"{year}年{month}月の消費者物価指数データ（中分類指数/全国/月次）を取得しています...")
    
    # URLを生成
    target_url = generate_url(year, month)
    print(f"アクセスするURL: {target_url}")
    
    # ターゲットページの内容を取得（最大3回試行）
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            response = requests.get(target_url, timeout=30)
            response.raise_for_status()
            break
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            if attempt < max_attempts - 1:
                wait_time = 5 * (attempt + 1)
                print(f"接続エラー: {e} - {wait_time}秒後に再試行します（{attempt+1}/{max_attempts}）")
                time.sleep(wait_time)
            else:
                print(f"接続エラー: {e} - 最大試行回数に達しました")
                return None
    
    # HTMLを解析
    soup = BeautifulSoup(response.content, 'html.parser')

    # 検索結果0件のページを検出する
    # 月コードが不正な場合、e-Statはエラーではなくこのページを200で返す
    page_text = soup.get_text()
    if '該当する統計データはありませんでした' in page_text or '0件のデータ' in page_text:
        print(f"検索結果が0件でした。指定した年月（{year}年{month}月）が未公表か、")
        print(f"URLパラメータが不正な可能性があります。")
        return None

    # 取得したページが目的の年月のものか確認する
    page_title = soup.title.get_text(strip=True) if soup.title else ''
    print(f"ページタイトル: {page_title}")
    if f'{year}年{month}月' not in page_title:
        print(f"ページタイトルに「{year}年{month}月」が含まれていません。")
        print(f"意図した年月と異なるページを取得している可能性があります。")
        return None

    # Excelダウンロードリンクを検索
    articles = soup.select(DATASET_ITEM_SELECTOR)
    print(f"データセット一覧の件数: {len(articles)}")
    excel_url = None

    # 方法1: 表番号1-1かつ周期が月次のレコードを探す
    # 表番号は完全一致で比較する。部分一致だと 11-1 や 21-1 を誤って拾う。
    # 周期の判定は必須。1-1には月次・年平均・年度平均の3レコードがあり、
    # これを見ないと年平均の統計表を掴む可能性がある。
    for article in articles:
        if extract_table_number(article) != '1-1':
            continue
        if not has_cycle(article, '月次'):
            continue
        link = find_excel_link(article)
        if link and link.get('href'):
            excel_url = base_url + link['href']
            print(f"表1-1（月次）からExcelリンクを見つけました")
            break

    # 方法2: 統計表名が中分類指数で周期が月次のレコードを探す
    if not excel_url:
        for article in articles:
            if '中分類指数' not in article.get_text():
                continue
            if not has_cycle(article, '月次'):
                continue
            link = find_excel_link(article)
            if link and link.get('href'):
                excel_url = base_url + link['href']
                print(f"中分類指数（月次）のExcelリンクを見つけました")
                break

    # 見つからない場合はここで失敗させる。
    # 以前はここでstatInfIdを算術的に予測するフォールバックがあったが、
    # e-StatのstatInfIdは連番ではないため無関係な統計表を掴み、
    # 正常なデータを古いもので上書きする事故を起こしていた。
    if excel_url:
        # ファイル名を設定
        excel_filename = f"CPI_中分類指数_全国_月次.xlsx"
        base_csv_name = f"CPI_中分類指数_全国_月次"
        excel_path = os.path.join(data_dir, excel_filename)
        
        print(f"Excelファイルをダウンロードしています: {excel_filename}")
        print(f"URL: {excel_url}")
        
        try:
            # Excelファイルをダウンロード
            excel_file = download_file(excel_url, excel_filename)
            print(f"ダウンロード完了: {excel_file}")

            # CSVを書き出す前に、中身が目的の年月の月次データか検証する。
            # 検証に失敗した場合は既存のCSVを一切上書きせずに終了する。
            if not verify_excel_contents(excel_file, year, month):
                os.remove(excel_file)
                return None

            # 複数のシートをCSVに変換
            print(f"すべてのシートをCSVに変換しています...")
            csv_files = convert_excel_to_csv(excel_file, base_csv_name)
            
            if csv_files:
                print(f"CSV変換完了。{len(csv_files)}個のCSVファイルを作成しました:")
                for csv_file in csv_files:
                    print(f"- {csv_file}")
                
                # 元のExcelファイルを削除（オプション）
                if len(csv_files) > 0:
                    try:
                        os.remove(excel_file)
                        print(f"元のExcelファイルを削除しました")
                    except Exception as e:
                        print(f"Excelファイル削除時にエラー: {e}")
                
                return csv_files
            else:
                print("CSV変換に失敗しました。")
                return None
        except Exception as e:
            print(f"ダウンロード中にエラーが発生しました: {e}")
    else:
        print("ダウンロードリンクが見つかりませんでした")
        dump_page_structure(soup)

    return None

if __name__ == "__main__":
    # 引数の処理（オプションで年月を指定可能）
    target_year = None
    target_month = None
    
    if len(sys.argv) > 2:
        try:
            target_year = int(sys.argv[1])
            target_month = int(sys.argv[2])
            print(f"指定された年月: {target_year}年{target_month}月")
        except ValueError:
            print("引数の形式が正しくありません。整数の年と月を指定してください。")
            print("例: python get_cpi.py 2025 2")
            sys.exit(1)
    
    # データ取得
    file_paths = download_cpi_data(target_year, target_month)
    
    if file_paths:
        print(f"消費者物価指数データを正常に取得しました:")
        for file_path in file_paths:
            print(f"- {file_path}")
        
        # 前月比のCSVを強調表示
        for file_path in file_paths:
            if "_前月比" in file_path:
                print(f"\n★ 前月比のデータ: {file_path}")
    else:
        print("データの取得に失敗しました")
        sys.exit(1)