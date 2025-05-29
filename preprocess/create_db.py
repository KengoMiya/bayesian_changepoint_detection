import pandas as pd
import sqlite3
import os
from tqdm import tqdm

def import_with_progress(import_file, db_name, table_name, chunksize=10000):
    """
    大きなCSVファイルをチャンクに分けて読み込み、SQLiteデータベースに
    インポートする関数。進捗状況も表示します。
    """
    try:
        # ファイルサイズの取得
        file_size = os.path.getsize(import_file)
        print(f"ファイルサイズ: {file_size/1024/1024:.2f} MB")

        # 読み込む前にファイルの行数を推定（高速に行うためヘッダーを考慮）
        with open(import_file, 'r', encoding='utf-8') as f:
            first_line = f.readline()  # ヘッダー行を読み込む
            # 区切り文字を自動検出
            if '\t' in first_line:
                sep = '\t'
            else:
                sep = ','

        # SQLiteデータベースに接続
        conn = sqlite3.connect(db_name)

        # 最初のチャンクを読み込んで列の型を確認
        print("スキーマを分析しています...")
        first_chunk = pd.read_csv(import_file, sep=sep, nrows=1000)
        dtypes = first_chunk.dtypes
        print("列の型情報:")
        for col, dtype in dtypes.items():
            print(f"  - {col}: {dtype}")

        # テーブルが存在する場合は削除
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()

        # reader オブジェクトを使ってチャンクごとに読み込む
        print(f"ファイル '{import_file}' を読み込んでいます...")
        
        # tqdmを使用して全体の進捗バーを表示
        with tqdm(total=file_size, unit='B', unit_scale=True, desc="インポート中") as pbar:
            # チャンクごとに読み込み
            chunks = pd.read_csv(import_file, sep=sep, chunksize=chunksize)
            
            for i, chunk in enumerate(chunks):
                # 最初のチャンクならテーブルを作成、以降は追加
                if_exists = 'replace' if i == 0 else 'append'
                
                # チャンクをデータベースに書き込む
                chunk.to_sql(table_name, conn, if_exists=if_exists, index=False)
                
                # 進捗バーを更新（おおよその進捗を表示）
                pbar.update(min(chunksize * len(chunk.columns) * 8, file_size - pbar.n))
                
                # 定期的に接続をコミット
                conn.commit()
        
        # 処理完了後のテーブル情報を取得
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        print(f"完了: {row_count}行のデータをデータベース '{db_name}' のテーブル '{table_name}' に正常にインポートされました。")
        
        # インデックスを作成（オプション）
        print("主要列にインデックスを作成しています...")
        try:
            for i, col in enumerate(first_chunk.columns[:3]):  # 最初の3列にインデックスを作成
                if first_chunk[col].dtype.name in ['int64', 'float64', 'datetime64[ns]']:
                    index_name = f"idx_{table_name}_{col}"
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({col})")
                    print(f"  - {col}列にインデックスを作成しました")
            conn.commit()
        except Exception as e:
            print(f"インデックス作成中にエラー: {e}")

    except FileNotFoundError:
        print(f"エラー: ファイル '{import_file}' が見つかりません。")
    except Exception as e:
        print(f"エラーが発生しました: {e}")
    finally:
        # データベース接続を閉じる
        if 'conn' in locals() and conn:
            conn.close()

# メイン処理
if __name__ == "__main__":
    import_file = r"c:\Users\kengo\Desktop\Bayesian_changepoint_detection\data\browserlog_201902_000"  # インポートするファイル
    db_name = r"c:\Users\kengo\Desktop\Bayesian_changepoint_detection\data\browser_log.db"  # データベースファイル
    table_name = "browserlog_201902"  # テーブル名
    
    # チャンクサイズの設定（メモリ使用量を調整）
    chunksize = 50000  # より大きなファイルの場合は小さくする
    
    import_with_progress(import_file, db_name, table_name, chunksize)