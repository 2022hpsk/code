from operator import le
import time
from utils.db import get_scheme
import json
from nodes.schema_link import get_schema_link
from nodes.classification import get_classification
from nodes.llm_medium import get_llm_medium_sql
from nodes.llm_hard import get_llm_hard_sql
from utils.util import execute_sql_with_pymysql, DecimalEncoder, eval_golden_sql
import sys

def process_sql(data: dict, config: dict):
    time_start = time.time()
    print(f"Starting processing sql {data.get('sql_id')}...")

    # get table scheme
    table_list = data.get("table_list")
    scheme_list = get_scheme(table_list, config.get("db"))

    # print(f"scheme_list: {scheme_list}")

    # get schema links
    schema_links = get_schema_link(table_list, scheme_list, data.get("question"), config.get("sse"), config.get("max_retry_times"))

    # print(f"schema_links: {schema_links}")

    if not schema_links:
        print(f"Get Schema Links failed for sql {data.get('sql_id')}")
        raise Exception(f"Get Schema Links failed for sql {data.get('sql_id')}")

    # get classification
    classification_result = get_classification(data.get("question"), scheme_list, schema_links, config.get("sse"), config.get("max_retry_times"))

    # print(f"classification_result: {classification_result}")

    if not classification_result or not classification_result.get("sub_questions"):
        print(f"Get Classification failed for sql {data.get('sql_id')}")
        raise Exception(f"Get Classification failed for sql {data.get('sql_id')}")

    current_round_sql = None
    last_round_sql = None
    last_round_error_message = None

    for i in range(config.get("max_retry_times")):
        try:
            current_round_sql = get_llm_hard_sql(query=data.get("question"), sub_questions=classification_result.get("sub_questions"), scheme=scheme_list, scheme_links=schema_links, knowledge=data.get("knowledge"), config=config.get("sse"), last_round_sql=last_round_sql, last_round_error_message=last_round_error_message)

            # execute sql
            sql_executor = execute_sql_with_pymysql()
            result = sql_executor.execute_sql_with_pymysql(current_round_sql, config.get("db"))

            time_end = time.time()
            print(f"Finished processing sql {data.get('sql_id')} in {time_end - time_start} seconds")

            return {
                "sql_id": data.get("sql_id"),
                "sql": current_round_sql,
                "status": "success",
                "result": result
            }
        
        except Exception as e:
            print(f"Error processing sql {data.get('sql_id')}: {e} in round {i+1}")
            last_round_sql = current_round_sql
            last_round_error_message = e
            time.sleep(1)

    print(f"Max retry times reached for sql {data.get('sql_id')}")
    return {
        "sql_id": data.get("sql_id"),
        "sql": current_round_sql,
        "status": "error",
        "error_message": "Max retry times reached"
    }

def test_single_sql(config: dict):
    test_data = {
        "sql_id": "sql_2",
        "question": "提取1个抽样号码包，2025.7.17-2025.7.23期间付费且不包含721号码包内的用户全量\n输出:gplayerid。",
        "复杂度": "中等",
        "table_list": [
            "dim_extract_311381_conf",
            "dws_argothek_ce1_login_di",
            "dws_argothek_ce1_cbt2_vplayerid_suserid_di"
        ],
        "knowledge": ""
    }
    result = process_sql(test_data, config)
    print(result)

def test_all_sql_and_save_result(config: dict):
    dataset_file_path = config.get("eval").get("dataset_file_path")
    result_file_path = config.get("eval").get("result_file_path")
    dataset = json.load(open(dataset_file_path, 'r'))

    result_list = []
    for data in dataset:
        result = process_sql(data, config)
        result_list.append(result)

        # save to result_file_path
        with open(result_file_path, 'w', encoding='utf-8') as f:
            json.dump(result_list, f, ensure_ascii=False, indent=4, cls = DecimalEncoder)

        break

    print(f"Finished processing {len(result_list)} sql and saved results to {result_file_path}")

def test_golden_sql(config: dict):
    dataset_file_path = config.get("eval").get("dataset_file_path")
    result_file_path = config.get("eval").get("result_file_path")
    dataset = json.load(open(dataset_file_path, 'r'))

    result_list = []
    for data in dataset:
        if data.get("golden_sql"):
            result = process_sql(data, config)
            print(result)
            result_list.append(result)

            # save to result_file_path
            with open(result_file_path, 'w', encoding='utf-8') as f:
                json.dump(result_list, f, ensure_ascii=False, indent=4, cls = DecimalEncoder)

    # eval result
    golden_sql_result_file_path = config.get("eval").get("golden_sql_result_file_path")
    golden_sql_result = json.load(open(golden_sql_result_file_path, 'r'))
    
    eval_result = eval_golden_sql(result_list, golden_sql_result)

    print(eval_result)

    print(f"Finished processing {len(result_list)} sql and saved results to {result_file_path}")


if __name__ == "__main__":
    config = json.load(open('config.json', 'r'))
    # test_single_sql(config=config)
    # test_all_sql_and_save_result(config=config)
    test_golden_sql(config=config)