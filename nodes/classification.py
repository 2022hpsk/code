from utils import call_sse


prompt = f"""

# For the given question, classify it as NESTED.
Always output Label: "NESTED".

# fields
{{scheme}}

# examples
Q: "统计各个玩法上线首周留存情况
输出：玩法、上线首周首次玩的日期、第几天留存（0,1,2...7)、玩法留存用户数

各玩法首周上线日期：
"广域战场": "20240723",
"消灭战": "20230804",
"幻想混战": "20241115",
"荒野传说": "20240903",
"策略载具": "20241010",
"炎夏混战": "20240625",
"单人装备": "20240517",
"交叉堡垒": "20240412""
schema_links: [dws_jordass_mode_roundrecord_di.dtstatdate,dws_jordass_mode_roundrecord_di.vplayerid,dws_jordass_mode_roundrecord_di.submodename,dws_jordass_mode_roundrecord_di.modename,dws_jordass_mode_roundrecord_di.mapname]
A: Let’s think step by step. The SQL query for the question "统计各个玩法上线首周留存情况..." needs these tables = [dws_jordass_mode_roundrecord_di], so we only reference a single base table but will use multiple derived subqueries and unions to separate modes.
Plus, it requires nested/derived queries and UNION ALL across mode-specific selections to build per-mode first-play dates and per-mode daily activity windows, and then a join between those derived results to compute day-diff retention; the sub-questions needed = ["对于每个玩法和每个玩家，在该玩法上线首周内的首次玩的日期是什么？","对于每个玩法，在首周后第0~7天哪些玩家有行为？"].
So, we need derived subqueries (UNION ALL) and a LEFT JOIN between derived sets, then the SQL query can be classified as "NESTED".
Label: "NESTED"

Q: "统计2019.5.8至2025.3.30 分月的玩法主玩情况
输出：月份(201905、201906、...、202503)、主玩玩法、主玩人数、总参与人数"
schema_links: [dws_jordass_mode_roundrecord_di.dtstatdate,dws_jordass_mode_roundrecord_di.vplayerid,dws_jordass_mode_roundrecord_di.roundtime,dwd_jordass_playerexitgamerecord_hi.tdbank_imp_date,dwd_jordass_playerexitgamerecord_hi.mode]
A: Let’s think step by step. The SQL query for the question "统计2019.5.8至2025.3.30 分月的玩法主玩情况..." needs these tables = [dws_jordass_mode_roundrecord_di, dwd_jordass_playerexitgamerecord_hi], so we need to combine data from two tables (UNION ALL) and aggregate per player-month-mode.
Plus, it requires intermediate aggregation per (mons, vplayerid, mode) to compute total roundtime/roundcnt and then windowing (row_number) to pick each player's main mode per month; the sub-question needed = ["对于每个玩家每个月，计算其在每个玩法的累计时长与对局数以确定主玩玩法。"].
So, we need CTE/aggregation + row_number windowing and UNION of sources, then the SQL query can be classified as "NESTED".
Label: "NESTED"

Q: "统计2024年12月14日-2024年12月20日参与建筑争夺玩法的玩家人数（去重）有多少，其中多少人是12月13日及以前有参与过建筑争夺的老玩家，有多少是新玩家
输出: 玩家人数、老玩家人数、新玩家人数"
schema_links: [dws_jordass_matchlog_stat_di.dtstatdate,dws_jordass_matchlog_stat_di.vplayerid,dws_jordass_playermatchrecord_stat_df.dtstatdate,dws_jordass_playermatchrecord_stat_df.vplayerid]
A: Let’s think step by step. The SQL query for the question "统计2024年12月14日-2024年12月20日参与建筑争夺玩法的玩家人数..." needs these tables = [dws_jordass_matchlog_stat_di, dws_jordass_playermatchrecord_stat_df], so we need JOIN between the set of players in the target window and the set of players who played earlier.
Plus, it requires a subquery to identify the target-window distinct players and a subquery to identify prior players (on 20241213 or earlier), then a LEFT JOIN to count existing vs new; the sub-question needed = ["在2024-12-14到2024-12-20期间，哪些玩家参与了建筑争夺？","在2024-12-13及以前，哪些玩家曾参与建筑争夺？"].
So, we need subqueries and LEFT JOIN/EXCEPT-style logic, then the SQL query can be classified as "NESTED".
Label: "NESTED"
Q 是问题，A是回答

Q：
{{query}}

schema_links：

{{scheme_links}}

A: Let’s think step by step...

按照`examples`输出 A

"""

def _parse_classification(response: dict):
    content = response['content']
    try:
        sub_questions = content.split('needed = ["')[1].split('"]')[0]
        flag = 'NESTED'
    except:
        sub_questions = ''
        flag = 'NON-NESTED'
    return {
        "sub_questions": sub_questions,
        "flag":flag
    }

def get_classification(query: str, scheme: str, scheme_links: str, config: dict):
    formatted_prompt = prompt.format(query=query, scheme=scheme, scheme_links=scheme_links)
    response = call_sse(config['app_key'], '', formatted_prompt)
    classification_result = _parse_classification(response)
    return classification_result