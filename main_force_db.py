import sqlite3
import json
import pandas as pd
from datetime import datetime
import os
import io

DB_PATH = 'main_force_history.db'

def init_db():
    """初始化数据库"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS main_force_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                params TEXT,
                result TEXT,
                analyzer_data TEXT
            )
        ''')
        conn.commit()
    except Exception as e:
        print(f"Database initialization error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

def save_analysis(result, analyzer, params):
    """保存分析结果"""
    init_db()
    
    try:
        # 序列化result
        # result中包含DataFrame，需要特殊处理
        # 假设 result['final_recommendations'] 是 list of dict，其中包含 'stock_data' (dict)
        # 应该可以直接json序列化
        
        # analyzer对象不能直接序列化，我们需要提取关键数据
        # 主要保存 raw_stocks (DataFrame) 和 三大分析师报告
        analyzer_data = {
            'fund_flow_analysis': getattr(analyzer, 'fund_flow_analysis', ''),
            'industry_analysis': getattr(analyzer, 'industry_analysis', ''),
            'fundamental_analysis': getattr(analyzer, 'fundamental_analysis', ''),
        }
        
        # 将DataFrame转换为JSON字符串
        if analyzer.raw_stocks is not None:
            # 使用 orient='records' 保存为 JSON 字符串
            analyzer_data['raw_stocks'] = analyzer.raw_stocks.to_json(orient='records', force_ascii=False)
        else:
            analyzer_data['raw_stocks'] = None
            
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            'INSERT INTO main_force_analysis (timestamp, params, result, analyzer_data) VALUES (?, ?, ?, ?)',
            (
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                json.dumps(params, ensure_ascii=False),
                json.dumps(result, ensure_ascii=False), # result中的recommendations是list of dict，可以直接序列化
                json.dumps(analyzer_data, ensure_ascii=False)
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
        c.execute('SELECT id, timestamp, params FROM main_force_analysis ORDER BY id DESC')
        rows = c.fetchall()
        conn.close()
        
        history = []
        for row in rows:
            try:
                params = json.loads(row[2])
            except:
                params = {}
            history.append({
                'id': row[0],
                'timestamp': row[1],
                'params': params
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
        c.execute('SELECT result, analyzer_data, timestamp, params FROM main_force_analysis WHERE id = ?', (record_id,))
        row = c.fetchone()
        conn.close()
        
        if not row:
            return None, None, None, None
            
        result_json = row[0]
        analyzer_data_json = row[1]
        timestamp = row[2]
        params = json.loads(row[3])
        
        result = json.loads(result_json)
        analyzer_data = json.loads(analyzer_data_json)
        
        # 恢复raw_stocks为DataFrame
        if analyzer_data.get('raw_stocks'):
            # 从JSON字符串读取DataFrame
            # 注意：这里我们使用 io.StringIO 来包装 JSON 字符串
            analyzer_data['raw_stocks'] = pd.read_json(io.StringIO(analyzer_data['raw_stocks']), orient='records')
            
        return result, analyzer_data, timestamp, params
    except Exception as e:
        print(f"Error loading analysis result: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None, None

def delete_analysis_record(record_id):
    """删除指定记录"""
    init_db()
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('DELETE FROM main_force_analysis WHERE id = ?', (record_id,))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error deleting record: {e}")
        return False
