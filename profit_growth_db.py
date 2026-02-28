import sqlite3
import json
import pandas as pd
from datetime import datetime
import os
import io

DB_PATH = os.path.join('data', 'profit_growth_history.db')

# 确保存储目录存在
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

def init_db():
    """初始化数据库"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS profit_growth_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                params TEXT,
                result TEXT,
                stocks_data TEXT
            )
        ''')
        conn.commit()
    except Exception as e:
        print(f"Database initialization error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

def save_analysis(stocks_df, top_n):
    """保存分析结果"""
    init_db()
    
    try:
        # 准备参数
        params = {
            'top_n': top_n
        }
        
        # 准备结果摘要
        result_summary = {
            'total_stocks': len(stocks_df),
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # 将DataFrame转换为JSON字符串
        if stocks_df is not None:
            # 使用 orient='records' 保存为 JSON 字符串
            stocks_data = stocks_df.to_json(orient='records', force_ascii=False)
        else:
            stocks_data = None
            
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            'INSERT INTO profit_growth_analysis (timestamp, params, result, stocks_data) VALUES (?, ?, ?, ?)',
            (
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                json.dumps(params, ensure_ascii=False),
                json.dumps(result_summary, ensure_ascii=False),
                stocks_data
            )
        )
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error saving analysis: {e}")
        return False

def get_history_list():
    """获取历史记录列表"""
    init_db()
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT id, timestamp, params, result FROM profit_growth_analysis ORDER BY id DESC')
        rows = c.fetchall()
        conn.close()
        
        history = []
        for row in rows:
            try:
                params = json.loads(row[2])
                result = json.loads(row[3])
            except:
                params = {}
                result = {}
                
            history.append({
                'id': row[0],
                'timestamp': row[1],
                'params': params,
                'result': result
            })
        return history
    except Exception as e:
        print(f"Error getting history list: {e}")
        return []

def get_analysis_result(record_id):
    """获取指定记录的分析结果"""
    init_db()
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT stocks_data, timestamp, params FROM profit_growth_analysis WHERE id = ?', (record_id,))
        row = c.fetchone()
        conn.close()
        
        if not row:
            return None, None, None
            
        stocks_data_json = row[0]
        timestamp = row[1]
        params = json.loads(row[2])
        
        # 恢复DataFrame
        stocks_df = None
        if stocks_data_json:
            # 从JSON字符串读取DataFrame
            stocks_df = pd.read_json(io.StringIO(stocks_data_json), orient='records')
            
        return stocks_df, timestamp, params
    except Exception as e:
        print(f"Error loading analysis result: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None

def delete_analysis_record(record_id):
    """删除指定记录"""
    init_db()
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('DELETE FROM profit_growth_analysis WHERE id = ?', (record_id,))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error deleting record: {e}")
        return False