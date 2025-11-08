import time
from nodes.goldensql_references import get_goldensql_references
from db import get_scheme
import json
from nodes.schema_link import get_schema_link
from nodes.classification import get_classification
from nodes.llm_medium import get_llm_medium_sql
from nodes.llm_hard import get_llm_hard_sql
from utils import execute_sql_with_pymysql
from utils import DecimalEncoder
import sys
def process_sql(data: dict, config: dict):
    time_start = time.time()

    print(f"Starting processing sql {data.get('sql_id')}...")
    # get table scheme
    table_list = data.get("table_list")
    scheme_list = get_scheme(table_list, config.get("db"))

    print(f"scheme_list: {scheme_list}")

    # get schema links
    schema_links = get_schema_link(table_list, scheme_list, data.get("question"), config.get("sse"))

    print(f"schema_links: {schema_links}")

    #get goldensql references:   [xx,xx,xx]
    goldensql_ids = get_goldensql_references(data.get("question"), config.get("sse"))



    # get classification
    classification_result = get_classification(data.get("question"), scheme_list, schema_links,goldensql_ids, config.get("sse"))

    print(f"classification_result: {classification_result}")

    # if classification_result.get("flag") == "NESTED":
    #     # get sub questions
    #     sub_questions = classification_result.get("sub_questions","")
    #     # get sub questions sql
    #     sql = get_llm_hard_sql(query=data.get("question"), sub_questions=sub_questions, scheme=scheme_list, scheme_links=schema_links, knowledge=data.get("knowledge"), config=config.get("sse"))
    # else:
    #     # get sql
    #     sql = get_llm_medium_sql(query=data.get("question"), scheme=scheme_list, scheme_links=schema_links, knowledge=data.get("knowledge"), config=config.get("sse"))

    # 统一使用 hard 模式
    sub_questions = classification_result.get("sub_questions","")
    sql = get_llm_hard_sql(query=data.get("question"), sub_questions=sub_questions, scheme=scheme_list, scheme_links=schema_links, knowledge=data.get("knowledge"), goldensql_ids=goldensql_ids,config=config.get("sse"))

    print(f"sql: {sql}")


    # execute sql
    sql_executor = execute_sql_with_pymysql()
    result = sql_executor.execute_sql_with_pymysql(sql, config.get("db"))

    print(result)

    time_end = time.time()
    print(f"Finished processing sql {data.get('sql_id')} in {time_end - time_start} seconds")

    return {
        "sql_id": data.get("sql_id"),
        "sql": sql,
        "status": "success",
        "result": result
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
    # 清空/创建结果文件（将采用每行一个 JSON 对象的流式存储）
    # with open(result_file_path, 'w', encoding='utf-8') as f:
    #     pass

    for data in dataset:
        result = process_sql(data, config)
        print(result)
        # 以追加方式将每个结果写为一行 JSON（ndjson 格式）
        with open(result_file_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(result, ensure_ascii=False, indent = 4, cls = DecimalEncoder) + '\n')

    print(f"Saved results (streamed) to {result_file_path}")

if __name__ == "__main__":
    # 打开文件并重定向 stdout
    with open('log.txt', 'w', buffering=1) as f:  # buffering=1 表示行缓冲
        sys.stdout = f
        sys.stderr = f  # 如果也想捕获错误输出

        config = json.load(open('config.json', 'r'))
        # test_single_sql(config=config)
        test_all_sql_and_save_result(config=config)


