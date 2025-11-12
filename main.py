from operator import le
import time
from utils.db import get_scheme
import json
from nodes.schema_link import get_schema_link
from nodes.classification import get_classification
from nodes.llm_hard import get_llm_hard_sql
from nodes.rewriter import rewrite_failed_sql
from utils.util import execute_sql_with_pymysql, DecimalEncoder, eval_golden_sql
import sys

def process_sql(data: dict, config: dict):
    time_start = time.time()
    print(f"Starting processing sql {data.get('sql_id')}...")

    # get table scheme
    table_list = data.get("table_list")
    # print(f"table_list: {table_list}")

    scheme_list = get_scheme(table_list, config)
    # print(f"scheme_list: {scheme_list}")

    # get schema links
    schema_links = get_schema_link(table_list, scheme_list, data, config)
    # print(f"schema_links: {schema_links}")

    # get classification
    classification_result = get_classification(scheme_list, schema_links, data, config)
    # print(f"classification_result: {classification_result}")

    for i in range(config.get("max_retry_times")):
        try:
            current_round_sql = get_llm_hard_sql(sub_questions=classification_result["sub_questions"], scheme=scheme_list, scheme_links=schema_links, data=data, config=config)

            # execute sql
            try:
                print(f"Starting to execute sql: {current_round_sql}")
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
                error_message = str(e)
                print(f"SQL execution failed in round {i+1}: {error_message}, start rewriting...")
                
                try:
                    print(f"Attempting to rewrite SQL using rewriter...")
                    current_round_sql = rewrite_failed_sql(
                        sub_questions=classification_result["sub_questions"],
                        scheme=scheme_list,
                        scheme_links=schema_links,
                        failed_sql=current_round_sql,
                        error_message=error_message,
                        data=data,
                        config=config
                    )
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
                except Exception as rewrite_error:
                    raise rewrite_error

        except Exception as e:
            print(f"Error processing sql {data.get('sql_id')}: {e} in round {i+1}")
            time.sleep(1)

    print(f"Max retry times reached for sql {data.get('sql_id')}")
    return {
        "sql_id": data.get("sql_id"),
        "sql": current_round_sql,
        "status": "error",
        "error_message": "Max retry times reached"
    }

def test_single_sql(config: dict):
    # test_data = {
    #     "sql_id": "sql_28",
    #     "question": "统计各个玩法上线首周留存情况\n输出：玩法、上线首周首次玩的日期、第几天留存（0,1,2...7)、玩法留存用户数\n\n各玩法首周上线日期：\n\"广域战场\": \"20240723\",\n\"消灭战\": \"20230804\",\n\"幻想混战\": \"20241115\",\n\"荒野传说\": \"20240903\",\n\"策略载具\": \"20241010\",\n\"炎夏混战\": \"20240625\",\n\"单人装备\": \"20240517\",\n\"交叉堡垒\": \"20240412\"",
    #     "sql": "select  a.itype,\n        a.dtstatdate,\n        datediff(b.dtstatdate,a.dtstatdate) as idaynum,\n        count(distinct a.vplayerid)           as iusernum\nfrom (                      \n    select\n        itype,\n        min(dtstatdate) as dtstatdate,\n        vplayerid\n    from  (\n        select '广域战场'      as itype,\n                min(dtstatdate) as dtstatdate,\n                vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240723' and dtstatdate <= date_add('20240723',6)\n        and submodename = '广域战场模式'\n        group by vplayerid\n\n        union all\n        select '消灭战', min(dtstatdate), vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20230804' and dtstatdate <= date_add('20230804',6)\n        and modename='组队竞技' and submodename like '%消灭战模式%'\n        group by vplayerid\n\n        union all\n        select '幻想混战', min(dtstatdate), vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20241115' and dtstatdate <= date_add('20241115',6)\n        and modename='创意创作间' and submodename='幻想混战'\n        group by vplayerid\n\n        union all\n        select '荒野传说', min(dtstatdate), vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240903' and dtstatdate <= date_add('20240903',6)\n        and modename='休闲模式' and submodename in ('荒野传说','荒野沙漠')\n        group by vplayerid\n\n        union all\n        select '策略载具', min(dtstatdate), vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20241010' and dtstatdate <= date_add('20241010',6)\n        and modename='休闲模式' and submodename like '%策略载具%'\n        group by vplayerid\n\n        union all\n        select '炎夏混战', min(dtstatdate), vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240625' and dtstatdate <= date_add('20240625',6)\n        and modename='创意创作间' and submodename like '%炎夏混战%'\n        group by vplayerid\n\n        union all\n        select '单人装备', min(dtstatdate), vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240517' and dtstatdate <= date_add('20240517',6)\n        and modename='组队竞技' and submodename like '%单人装备%'\n        group by vplayerid\n\n        union all\n        select '交叉堡垒', min(dtstatdate), vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240412' and dtstatdate <= date_add('20240412',6)\n        and modename='组队竞技' and submodename like '%交叉堡垒%'\n        group by vplayerid\n    ) t\n    group by itype, vplayerid\n) a\nleft join (\n        select '广域战场' as itype, dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240723' and dtstatdate <= date_add('20240723',13)\n          and submodename = '广域战场模式'\n        group by dtstatdate, vplayerid\n\n        union all\n        select '消灭战', dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20230804' and dtstatdate <= date_add('20230804',13)\n          and modename='组队竞技' and submodename like '%消灭战模式%'\n        group by dtstatdate, vplayerid\n\n        union all\n        select '幻想混战', dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20241115' and dtstatdate <= date_add('20241115',13)\n          and modename='创意创作间' and submodename='幻想混战'\n        group by dtstatdate, vplayerid\n\n        union all\n        select '荒野传说', dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240903' and dtstatdate <= date_add('20240903',13)\n          and modename='休闲模式' and submodename in ('荒野传说','荒野沙漠')\n        group by dtstatdate, vplayerid\n\n        union all\n        select '策略载具', dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20241010' and dtstatdate <= date_add('20241010',13)\n          and modename='休闲模式' and submodename like '%策略载具%'\n        group by dtstatdate, vplayerid\n\n        union all\n        select '炎夏混战', dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240625' and dtstatdate <= date_add('20240625',13)\n          and modename='创意创作间' and submodename like '%炎夏混战%'\n        group by dtstatdate, vplayerid\n\n        union all\n        select '单人装备', dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240517' and dtstatdate <= date_add('20240517',13)\n          and modename='组队竞技' and submodename like '%单人装备%'\n        group by dtstatdate, vplayerid\n\n        union all\n        select '交叉堡垒', dtstatdate, vplayerid\n        from dws_jordass_mode_roundrecord_di\n        where dtstatdate >= '20240412' and dtstatdate <= date_add('20240412',13)\n          and modename='组队竞技' and submodename like '%交叉堡垒%'\n        group by dtstatdate, vplayerid\n) b\n  on  a.itype      = b.itype\nand  a.vplayerid    = b.vplayerid\nwhere datediff(b.dtstatdate,a.dtstatdate) between 0 and 7\ngroup by a.itype, a.dtstatdate, datediff(b.dtstatdate,a.dtstatdate);\n",
    #     "复杂度": "中等",
    #     "table_list": [
    #         "dws_jordass_mode_roundrecord_di"
    #     ],
    #     "knowledge": "说明：\n广域战场 （2024/7/23）submodename= '广域战场模式'，\n消灭战（2023/8/4） modename='组队竞技' and submodename like '%消灭战模式%'，\n幻想混战（2024/11/15）modename='创意创作间' and submodename='幻想混战'，\n荒野传说（2024-09-03）modename='休闲模式' and submodename in ('荒野传说','荒野沙漠')，\n策略载具（2024-10-10）modename='休闲模式' and submodename like '%策略载具%'，\n炎夏混战（2024-06-25）modename='创意创作间' and submodename like '%炎夏混战%'，\n单人装备（2024.5.17）modename='组队竞技' and submodename like '%单人装备%'，\n交叉堡垒（2024.4.12） modename='组队竞技' and submodename like '%交叉堡垒%'\n\n第几天留存：0表示当天参与、1表示当天参与在第2天也参与、2表示当天参与在第3天也参与，依此类推",
    #     "golden_sql": True
    # }

    test_data = {
        "sql_id": "sql_30",
        "question": "统计2019.5.8至2025.3.30 分月的玩法主玩情况\n输出：月份(201905、201906、...、202503)、主玩玩法、主玩人数、总参与人数",
        "sql": "with main_user as (\n    select substr(dtstatdate, 1, 6) mons,\n        case\n            when modename = '传统模式' and submodename like 'CG%' and mapname = '群屿' then '主题群屿'\n            when modename = '传统模式' and mapname = '群屿' then '传统群屿'\n            when modename = '传统模式' and mapname = '假日群岛' then '假日群岛'\n            when modename = '传统模式' and mapname = '荣耀之城' then '荣耀之城'\n            when submodename = '广域战场模式' then '广域战场'\n            when submodename = '极能形态模式' then '极能形态'\n            when modename = '组队竞技' then '组竞'\n            when modename = '乐园' then '乐园'\n            when modename = '领地' then '领地'\n            when modename = '广阔天地' then '广阔天地'\n            else '其他模式'\n        end imodename,\n        vplayerid,\n        sum(roundtime) / 60 roundtime,\n        sum(roundcnt) roundcnt\n   from dws_jordass_mode_roundrecord_di\n   where dtstatdate between '20190508' and '20250330'\n   group by vplayerid, substr(dtstatdate, 1, 6),\n        case\n            when modename = '传统模式' and submodename like 'CG%' and mapname = '群屿' then '主题群屿'\n            when modename = '传统模式' and mapname = '群屿' then '传统群屿'\n            when modename = '传统模式' and mapname = '假日群岛' then '假日群岛'\n            when modename = '传统模式' and mapname = '荣耀之城' then '荣耀之城'\n            when submodename = '广域战场模式' then '广域战场'\n            when submodename = '极能形态模式' then '极能形态'\n            when modename = '组队竞技' then '组竞'\n            when modename = '乐园' then '乐园'\n            when modename = '领地' then '领地'\n            when modename = '广阔天地' then '广阔天地'\n            else '其他模式'\n        end\n\n   union all\n   \n   select substr (tdbank_imp_date, 1, 6) mons,\n        '隧道' as modename,\n        vplayerid,\n        sum(roundtime) / 60 roundtime,\n        count(distinct gameid) roundcnt\n   from dwd_jordass_playerexitgamerecord_hi\n   where tdbank_imp_date between '2024061800' and '2025033023'\n     and mode in (3001, 3002, 3003)\n   group by substr (tdbank_imp_date, 1, 6), vplayerid\n)\n\nselect a.mons,\n       a.imodename,\n       a.iusernum,\n       b.iusernumall\nfrom (\n    select mons,\n          imodename,\n          count(vplayerid) iusernum\n    from (\n        select mons, vplayerid, imodename\n        from (\n            select mons,\n                vplayerid,\n                imodename,\n                roundtime,\n                row_number() over (partition by mons, vplayerid order by roundtime desc) as rn\n            from main_user\n        )ff1\n        where rn = 1\n    )f1\n   group by mons, imodename\n) a\nleft join (\n    select mons,\n        imodename,\n        count(vplayerid) iusernumall\n   from main_user\n   group by mons, imodename\n) b \non a.imodename = b.imodename and a.mons = b.mons\norder by a.mons, a.imodename\n;",
        "复杂度": "复杂",
        "table_list": [
            "dwd_jordass_playerexitgamerecord_hi",
            "dws_jordass_mode_roundrecord_di"
        ],
        "knowledge": "主玩定义：用户累计对局时长最多的玩法；\n\n玩法：从dws_jordass_mode_roundrecord_di取\n从上往下匹配，如果满足以下任一条件，则返回对应的名称，否则返回“其他模式”。\n如果模式为“传统模式”，且子模式名称以“CG”开头，且地图为“群屿”，则返回“主题群屿”；\n如果模式为“传统模式”且地图为“群屿”，则返回“传统群屿”；\n如果模式为“传统模式”且地图为“假日群岛”，则返回“假日群岛”；\n如果模式为“传统模式”且地图为“荣耀之城”，则返回“荣耀之城”；\n如果子模式是“广域战场模式”，则返回“广域战场”；\n如果子模式是“极能形态模式”，则返回“极能形态”；\n如果模式是“组队竞技”，则返回“组竞”；\n如果模式是“休闲模式”，则返回“休闲”；\n如果模式是“乐园”，则返回“乐园”；\n如果模式是“领地”，则返回“领地”；\n如果模式是“广阔天地”，则返回“广阔天地”\n\n注意，参与隧道模式的用户数据从dwd_jordass_playerexitgamerecord_hi取，时间限制 2024.6.18-2025.3.30，mode in (3001, 3002, 3003)",
        "golden_sql": True
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
            # print(result)
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

def load_config(file_path="config.json", common_knowledge_path="common_knowledge.txt"):
    # 读取配置文件
    with open(file_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    # 读取常识内容
    with open(common_knowledge_path, 'r', encoding='utf-8') as f:
        common_knowledge = f.read()
    # 放到config中
    config['common_knowledge'] = common_knowledge
    return config


if __name__ == "__main__":
    config = load_config()
    # test_single_sql(config=config)
    # test_all_sql_and_save_result(config=config)
    test_golden_sql(config=config)