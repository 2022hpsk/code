import time
import traceback
from utils.util import call_sse

prompt = f"""
# Task
你是一个SQL专家，你要根据问题生成对应的MySQL查询语句。
我会给你提供这个问题相关的表的scheme、scheme_links和sub-questions，你需要根据全部信息生成最终语法正确的、最合理的MySQL语句。

# Common knowledge
这是一些背景信息
{{common_knowledge}}

# Question knowledge
这是一些与当前问题相关的信息
{{question_knowledge}}

# Example
Question: 统计2019.5.8至2025.3.30 分月的玩法主玩情况\n输出：月份(201905、201906、...、202503)、主玩玩法、主玩人数、总参与人数"

Table scheme: 

Schema links: [dws_jordass_mode_roundrecord_di.dtstatdate, 
dws_jordass_mode_roundrecord_di.vplayerid, 
dws_jordass_mode_roundrecord_di.mode, 
dws_jordass_mode_roundrecord_di.modename, 
dws_jordass_mode_roundrecord_di.submode, 
dws_jordass_mode_roundrecord_di.submodename, 
dws_jordass_mode_roundrecord_di.mapname, 
dws_jordass_mode_roundrecord_di.roundtime,
dwd_jordass_playerexitgamerecord_hi.dteventtime,
dwd_jordass_playerexitgamerecord_hi.vplayerid,
dwd_jordass_playerexitgamerecord_hi.mode,
dwd_jordass_playerexitgamerecord_hi.roundtime,
"2019-05-08",
"2025-03-30"]

Answer: Let's think step by step. "统计2019.5.8至2025.3.30 分月的玩法主玩情况\n输出：月份(201905、201906、...、202503)、主玩玩法、主玩人数、总参与人数" can be solved by knowing the answer to the following sub-questions ['How to determine the main play mode for each player in each month?', 'How to calculate the total participation count for each month?', 'How to handle the special case of tunnel mode players (mode in (3001, 3002, 3003)) during 2024.6.18-2025.3.30?', 'How to apply the play mode classification rules to determine the final mode name?', 'How to aggregate the results by month and main play mode?'] To solve this question, we first aggregate data by month using substr(dtstatdate,1,6) for DWS data and substr(tdbank_imp_date,1,6) for DWD data. Then we classify each play mode according to the given mapping rules: if the mode is “traditional” and submode starts with “CG” and map is “群屿”, we label it as “主题群屿”; if the mode is “traditional” and map is “群屿”, “假日群岛”, or “荣耀之城”, we label it accordingly as “传统群屿”, “假日群岛”, or “荣耀之城”; if the submode is “广域战场模式” or “极能形态模式”, we map them to “广域战场” and “极能形态”; if the mode is “组队竞技”, “乐园”, “领地”, or “广阔天地”, we map them directly to “组竞”, “乐园”, “领地”, and “广阔天地”; all others become “其他模式”. Next, we build a detailed record of player participation: from dws_jordass_mode_roundrecord_di we sum the total round time and round count for each player, month, and mapped mode between 2019-05-08 and 2025-03-30; from dwd_jordass_playerexitgamerecord_hi we add tunnel mode data (mode in 3001, 3002, 3003) for the range 2024-06-18 to 2025-03-30, labeling the mode as “隧道”. We union these two datasets into one called main_user. Then we determine each player’s main mode per month by ranking modes by total round time (descending) and selecting the top one. The number of main players per mode per month is counted from these top records, while total participants per mode per month are counted from all records. Finally, we join the two results on month and mode, and output month, main mode name, main player count, and total participant count sorted by month and mode. So the final SQL is: ```sql
with main_user as (\n    select substr(dtstatdate, 1, 6) mons,\n        case\n            when modename = '传统模式' and submodename like 'CG%' and mapname = '群屿' then '主题群屿'\n            when modename = '传统模式' and mapname = '群屿' then '传统群屿'\n            when modename = '传统模式' and mapname = '假日群岛' then '假日群岛'\n            when modename = '传统模式' and mapname = '荣耀之城' then '荣耀之城'\n            when submodename = '广域战场模式' then '广域战场'\n            when submodename = '极能形态模式' then '极能形态'\n            when modename = '组队竞技' then '组竞'\n            when modename = '乐园' then '乐园'\n            when modename = '领地' then '领地'\n            when modename = '广阔天地' then '广阔天地'\n            else '其他模式'\n        end imodename,\n        vplayerid,\n        sum(roundtime) / 60 roundtime,\n        sum(roundcnt) roundcnt\n   from dws_jordass_mode_roundrecord_di\n   where dtstatdate between '20190508' and '20250330'\n   group by vplayerid, substr(dtstatdate, 1, 6),\n        case\n            when modename = '传统模式' and submodename like 'CG%' and mapname = '群屿' then '主题群屿'\n            when modename = '传统模式' and mapname = '群屿' then '传统群屿'\n            when modename = '传统模式' and mapname = '假日群岛' then '假日群岛'\n            when modename = '传统模式' and mapname = '荣耀之城' then '荣耀之城'\n            when submodename = '广域战场模式' then '广域战场'\n            when submodename = '极能形态模式' then '极能形态'\n            when modename = '组队竞技' then '组竞'\n            when modename = '乐园' then '乐园'\n            when modename = '领地' then '领地'\n            when modename = '广阔天地' then '广阔天地'\n            else '其他模式'\n        end\n\n   union all\n   \n   select substr (tdbank_imp_date, 1, 6) mons,\n        '隧道' as modename,\n        vplayerid,\n        sum(roundtime) / 60 roundtime,\n        count(distinct gameid) roundcnt\n   from dwd_jordass_playerexitgamerecord_hi\n   where tdbank_imp_date between '2024061800' and '2025033023'\n     and mode in (3001, 3002, 3003)\n   group by substr (tdbank_imp_date, 1, 6), vplayerid\n)\n\nselect a.mons,\n       a.imodename,\n       a.iusernum,\n       b.iusernumall\nfrom (\n    select mons,\n          imodename,\n          count(vplayerid) iusernum\n    from (\n        select mons, vplayerid, imodename\n        from (\n            select mons,\n                vplayerid,\n                imodename,\n                roundtime,\n                row_number() over (partition by mons, vplayerid order by roundtime desc) as rn\n            from main_user\n        )ff1\n        where rn = 1\n    )f1\n   group by mons, imodename\n) a\nleft join (\n    select mons,\n        imodename,\n        count(vplayerid) iusernumall\n   from main_user\n   group by mons, imodename\n) b \non a.imodename = b.imodename and a.mons = b.mons\norder by a.mons, a.imodename\n;
```

# Input
这是你当前需要处理的数据，请输出完整的Answer，必须按照格式输出最终SQL。

Question: {{question}}

Table scheme: {{scheme}}

Schema_links: {{scheme_links}}

Answer: Let's think step by step. "{{question}}" can be solved by knowing the answer to the following sub-questions {{sub_questions}} ...
So the final SQL query is:
```sql
...
```
"""

def _parse_llm_hard(response: dict):
    content = response['content']
    sql = content.split('```sql')[1].split('```')[0]
    return sql

def get_llm_hard_sql(sub_questions: str, scheme: str, scheme_links: str, data: dict, config: dict):
    try:
        formatted_prompt = prompt.format(
            common_knowledge=config["common_knowledge"],
            question_knowledge=data["knowledge"],
            question=data["question"],
            sub_questions=sub_questions,
            scheme=scheme,
            scheme_links=scheme_links
        )

        # print(formatted_prompt)

        response = call_sse(config['sse']['app_key'], '', formatted_prompt)

        # print(response)

        sql = _parse_llm_hard(response)

        return sql

    except Exception as e:
        raise Exception(f"Get LLM Hard SQL failed for current sql")