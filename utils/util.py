import time
import requests
from typing import Dict, Optional, Callable, Iterator, List
import uuid
import json
import pymysql
import traceback
from decimal import Decimal
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Union
from collections import Counter
import math

def call_sse(
    bot_app_key: str,
    system_prompt: str,
    user_prompt: str,
    incremental: bool = True,
    streaming_throttle: int = 10,
    visitor_labels: list = None,
    search_network: str = "disable",
    stream: str = "enable",
    workflow_status: str = "disable",
    tcadp_user_id: str = "",
):
    """
    调用腾讯云 QBot 聊天 SSE API
    
    Args:
        bot_app_key: 应用对应的key
        system_prompt: 系统提示词
        user_prompt: 用户提示词
        visitor_biz_id: 访客业务ID，默认为session_id
        incremental: 是否增量返回
        streaming_throttle: 流式节流时间（毫秒）
        visitor_labels: 访客标签列表
        custom_variables: 自定义变量
        search_network: 搜索网络设置
        stream: 是否启用流式
        workflow_status: 工作流状态
        tcadp_user_id: TCADP用户ID
        on_message: 可选的回调函数，用于处理接收到的消息
        
    Yields:
        str: 从SSE流中接收到的消息内容
        
    Returns:
        Iterator[str]: 消息迭代器
    """
    url = "https://wss.lke.cloud.tencent.com/v1/qbot/chat/sse"
    
    headers = {
        "Content-Type": "application/json"
    }

    max_retries = 3
    
    temp_id = str(uuid.uuid4())
    payload = {
        "session_id": temp_id,
        "bot_app_key": bot_app_key,
        "visitor_biz_id": temp_id,
        "content": "CALL WORKFLOW",
        "incremental": incremental,
        "streaming_throttle": streaming_throttle,
        "visitor_labels": visitor_labels or [],
        "custom_variables": {
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
        },
        "search_network": search_network,
        "stream": stream,
        "workflow_status": workflow_status,
        "tcadp_user_id": tcadp_user_id
    }

    def process(data):
        try:
            if data['type'] == 'reply':
                payload = data['payload']
                if 'is_llm_generated' in payload and 'work_flow' in payload:
                    workflow_result_str = payload['work_flow']['current_node']['Output']
                    # 确保正确处理中文编码
                    if isinstance(workflow_result_str, bytes):
                        workflow_result_str = workflow_result_str.decode('utf-8')
                    # 使用 strict=False 允许更宽松的 JSON 解析
                    reuslt_obj = json.loads(workflow_result_str, strict=False)
                    return reuslt_obj
        except Exception as e:
            print(f"处理失败: {e}, data: {data}")

    for i in range(max_retries):
        try:
            response = requests.post(url, headers=headers, json=payload, stream=True)
            response.raise_for_status()
            
            # 确保响应使用 UTF-8 编码
            response.encoding = 'utf-8'

            data_buffer = ""  # 用于累积多个 data 行的内容
            result = None

            for msg in response.iter_lines(decode_unicode=True):
                if msg == "":
                    # 空行表示一个事件结束，尝试解析累积的数据
                    if data_buffer:
                        try:
                            data = json.loads(data_buffer, strict=False)
                            result = process(data)
                        except Exception as e:
                            print(f"Error: {e}")
                        finally:
                            data_buffer = ""  # 清空缓冲区
                    continue
                if msg.startswith("event:"):
                    continue
                elif msg.startswith("data:"):
                    # 累积 data 行的内容（可能有多行 data）
                    data_content = msg[5:].strip()
                    if data_buffer:
                        data_buffer += data_content
                    else:
                        data_buffer = data_content
                    
                    # 尝试解析，如果成功就处理，如果失败就继续累积
                    if data_buffer:
                        try:
                            data = json.loads(data_buffer, strict=False)
                            result = process(data)
                            data_buffer = ""  # 清空缓冲区
                        except json.JSONDecodeError:
                            # 解析失败，可能是数据还没完整，继续累积
                            pass
                else:
                    # 累积 data 行的内容（可能有多行 data）
                    data_buffer += msg
                    continue

            if result is None:
                print(f"result is None, retry {i} times")
                continue
            
            return result

        except requests.exceptions.RequestException as e:
            raise Exception(f"API调用失败: {str(e)}")


class DecimalEncoder(json.JSONEncoder):
    """
    自定义 JSON 编码器，用于处理 Decimal、datetime、date 类型。
    
    由于标准JSON编码器无法处理Decimal类型和日期时间类型，
    这个自定义编码器将这些特殊类型转换为JSON兼容的格式。
    """
    def default(self, obj):
        """
        重写default方法，处理特殊数据类型。
        
        Args:
            obj: 需要编码的对象
            
        Returns:
            编码后的值，如果无法处理则调用父类方法
        """
        if isinstance(obj, Decimal):
            # 检查 Decimal 值是否为整数（即小数点后全是零）
            # 如果是整数则转为int，否则转为float
            return int(obj) if obj == obj.to_integral_value() else float(obj)
        elif isinstance(obj, (datetime, date)):
            # 将日期时间对象转为ISO格式字符串
            return obj.isoformat()
        # 其他类型使用父类的默认处理方式
        return super().default(obj)

class execute_sql_with_pymysql:
    """
    SQL执行器类，用于通过pymysql连接MySQL数据库并执行SQL语句。
    """

    def __init__(self):
        pass

    def execute_sql_with_pymysql(self, sql: str, db_config: Dict):
        """
        执行SQL查询语句的主要方法。
        
        从输入JSON文件中读取SQL语句列表，连接到数据库执行这些SQL，
        并将执行结果保存到输出JSON文件中。
        
        Args:
            sql (str): 要执行的SQL语句
            db_config (dict): 数据库连接配置字典，包含host、user、password等
        """
        results = []  # 存储所有SQL执行结果的列表
        conn = None   # 数据库连接对象
        
        try:
            # 连接数据库
            conn = pymysql.connect(**db_config)
            # 使用DictCursor以便返回字典形式的结果
            cursor = conn.cursor(pymysql.cursors.DictCursor)

            # 执行SQL语句
            cursor.execute(sql)
            # 获取查询结果
            query_result = cursor.fetchall()
            # 对结果中的数字进行标准化处理
            query_result = self.normalize_numbers_in_result(query_result)
            
            return query_result

        except Exception as e:
            traceback.print_exc()
            print(f"执行SQL语句时发生错误：{e}")
            raise Exception(f"Execute SQL with pymysql failed: {e}")
    
        finally:
            if conn: # 确保数据库连接被关闭
                conn.close()
 
    def normalize_numbers_in_result(self, result_list: List[Dict]) -> List[Dict]:
        """
        对查询结果中的数字进行标准化处理 (使用生成式精简版)。
        
        遍历查询结果，将float类型中实际为整数的值转为int，否则保留两位小数。
        """
        
        # 内部辅助函数，用于处理单个键值对的标准化逻辑
        def _normalize_value(value):
            if isinstance(value, float):
                # 如果是浮点数但无小数部分，则转为整数
                if value.is_integer():
                    return int(value)
                else:
                    # 保留两位小数
                    return round(value, 2)
            if isinstance(value, Decimal): # 针对Decimal类型，同样保留两位小数
                return round(value, 2)
            else:
                # 其他类型保持原样
                return value

        # 使用列表生成式迭代行 (row)，内部使用字典生成式迭代列 (key, value)
        normalized = [
            {
                key: _normalize_value(value)
                for key, value in row.items()
            }
            for row in result_list
        ]
        
        return normalized


# --- 示例用法 ---
def test_execute_sql_with_pymysql():

    # 创建sql执行器对象
    sql_executor = execute_sql_with_pymysql()
    
    # 数据库连接配置
    db_configuration = {
        'host': '127.0.0.1',      # 数据库主机地址
        'user': 'root',      # 数据库用户名
        # 'password': 'Tencent.tgac', # 数据库密码
        'db': 'tgac', # 数据库名称
        'port': 9030 # starrocks访问端口
    }

    # 执行插入操作
    insert_file_path = "/mnt/tgac/data/insert_sql.json"
    insert_result_file_path = "/mnt/tgac/result/insert_exe_result.json"
    sql_executor.insert_data_with_pymysql(insert_file_path, insert_result_file_path, db_configuration)
    
    # 执行查询操作
    dataset_file_path = "/mnt/tgac/data/final_dataset.json"
    dataset_result_file_path = "/mnt/tgac/result/dataset_exe_result.json"
    sql_executor.execute_sql_with_pymysql(dataset_file_path, dataset_result_file_path, db_config = db_configuration)

# 使用示例
def test_call_sse():
    app_key = "dSPJGkcHWIEdnFcYENKRqoDhFDAfbrOocoZVdHbJUvdrzBdKqithdkXMdStiaHlvlGHajnWEKJpJeGeTrHSZNGbvEvJHNpEVBTQDqXDUntdHvebbWlfhWdhSELfpyPYH"
    system_prompt = "你是AI助手"
    user_prompt = "哈喽"

    total_times = 1000000
    for i in range(total_times):
        result = call_sse(app_key, system_prompt, user_prompt)
        print(result)


def eval_golden_sql(result_list: List[Dict], golden_sql_result: List[Dict]) -> Dict:
    eval_result = {
        "precision": 0,
        "correct_num": 0,
        "incorrect_num": 0,
        "total_num": len(result_list)
    }

    if not result_list:
        return eval_result

    def _normalize_atomic_value(value):
        if isinstance(value, bool):
            return ("bool", value)
        if isinstance(value, Decimal):
            normalized = value
        elif isinstance(value, (int,)):
            normalized = Decimal(value)
        elif isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                return ("number", str(value))
            normalized = Decimal(str(value))
        elif isinstance(value, (datetime, date)):
            return ("datetime", value.isoformat())
        elif value is None:
            return ("none", None)
        else:
            return ("str", str(value))

        normalized = normalized.normalize()
        return ("number", str(normalized))

    def _normalize_structure(value):
        if isinstance(value, dict):
            normalized_items = [_normalize_structure(v) for v in value.values()]
            normalized_items.sort(key=lambda item: repr(item))
            return ("dict", len(value), tuple(normalized_items))
        if isinstance(value, list):
            normalized_items = [_normalize_structure(v) for v in value]
            normalized_items.sort(key=lambda item: repr(item))
            return ("list", len(value), tuple(normalized_items))
        return _normalize_atomic_value(value)

    def _normalize_row(row):
        if isinstance(row, dict):
            normalized_values = [_normalize_structure(v) for v in row.values()]
            normalized_values.sort(key=lambda item: repr(item))
            return ("row", len(row), tuple(normalized_values))
        if isinstance(row, list):
            normalized_items = [_normalize_structure(v) for v in row]
            normalized_items.sort(key=lambda item: repr(item))
            return ("row_list", len(row), tuple(normalized_items))
        return ("row_value", _normalize_structure(row))

    def _compare_results(actual, golden):
        if isinstance(golden, list):
            if not isinstance(actual, list):
                return False, f"type mismatch: expected list, got {type(actual).__name__}"
            if len(actual) != len(golden):
                return False, f"row count mismatch: expected {len(golden)}, got {len(actual)}"

            actual_field_counts = Counter(len(row) if isinstance(row, dict) else None for row in actual)
            golden_field_counts = Counter(len(row) if isinstance(row, dict) else None for row in golden)
            if actual_field_counts != golden_field_counts:
                return False, "field count mismatch between results"

            actual_counter = Counter(_normalize_row(row) for row in actual)
            golden_counter = Counter(_normalize_row(row) for row in golden)
            if actual_counter != golden_counter:
                missing = list((golden_counter - actual_counter).elements())
                extra = list((actual_counter - golden_counter).elements())
                details = []
                if missing:
                    details.append(f"{len(missing)} expected row(s) missing")
                if extra:
                    details.append(f"{len(extra)} unexpected row(s) found")
                if not details:
                    details.append("row content mismatch")
                return False, "; ".join(details)
            return True, ""

        if isinstance(golden, dict):
            if not isinstance(actual, dict):
                return False, f"type mismatch: expected dict, got {type(actual).__name__}"
            if len(actual) != len(golden):
                return False, f"field count mismatch: expected {len(golden)}, got {len(actual)}"
            if _normalize_structure(actual) != _normalize_structure(golden):
                return False, "value mismatch in dict structure"
            return True, ""

        if _normalize_structure(actual) == _normalize_structure(golden):
            return True, ""
        return False, "value mismatch"

    golden_map = {
        item.get("sql_id"): item
        for item in (golden_sql_result or [])
        if isinstance(item, dict) and item.get("sql_id")
    }

    mismatch_details = []

    for item in result_list:
        sql_id = item.get("sql_id")
        if sql_id is None:
            eval_result["incorrect_num"] += 1
            mismatch_details.append({
                "sql_id": sql_id,
                "reason": "missing sql_id in result"
            })
            continue

        golden_item = golden_map.get(sql_id)
        if not golden_item:
            eval_result["incorrect_num"] += 1
            mismatch_details.append({
                "sql_id": sql_id,
                "reason": "missing golden result"
            })
            continue

        actual_status = item.get("status")
        golden_status = golden_item.get("status")
        if golden_status and actual_status and golden_status != actual_status:
            eval_result["incorrect_num"] += 1
            mismatch_details.append({
                "sql_id": sql_id,
                "reason": f"status mismatch: expected {golden_status}, got {actual_status}"
            })
            continue

        actual_result = item.get("result")
        golden_result = golden_item.get("result")
        is_match, reason = _compare_results(actual_result, golden_result)

        if is_match:
            eval_result["correct_num"] += 1
        else:
            eval_result["incorrect_num"] += 1
            mismatch_details.append({
                "sql_id": sql_id,
                "reason": reason or "result mismatch"
            })

    if eval_result["total_num"]:
        eval_result["precision"] = eval_result["correct_num"] / eval_result["total_num"]

    if mismatch_details:
        eval_result["details"] = mismatch_details

    return eval_result