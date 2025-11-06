from utils import call_sse


prompt = f"""


# Use the intermediate representation and the schema links to generate the SQL queries for each of the questions.
# all_fields

{{scheme}}


# plan

{{knowledge}}

#note
Many tables such as `dws_jordass_playermatchrecord_stat_df` are not detail records but rather ‘daily snapshot/daily summary tables’.
They record a cumulative player snapshot daily.
For `df` snapshot tables, `old` players can only be specified using `=` to denote the ‘snapshot boundary date



# examples 
(Please study the examples carefully, extract the essence of their usage, and pay close attention to the details)
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
Schema_links: [dws_jordass_mode_roundrecord_di.dtstatdate,dws_jordass_mode_roundrecord_di.vplayerid,dws_jordass_mode_roundrecord_di.submodename,dws_jordass_mode_roundrecord_di.modename]
A: Let's think step by step. "统计各个玩法上线首周留存情况..." can be solved by knowing the answer to the following sub-questions "对于每个玩法与每个玩家，首周内该玩家的首次玩的日期是什么？" 和 "在首周后的第0~7天中，该玩法哪些玩家有行为？".
The SQL query for the sub-question "对于每个玩法与每个玩家，首周内该玩家的首次玩的日期是什么？" is:

select
    itype,
    min(dtstatdate) as dtstatdate,
    vplayerid
from (
    select '广域战场'      as itype, min(dtstatdate) as dtstatdate, vplayerid
    from dws_jordass_mode_roundrecord_di
    where dtstatdate >= '20240723' and dtstatdate <= date_add('20240723',6)
      and submodename = '广域战场模式'
    group by vplayerid

    union all
    select '消灭战', min(dtstatdate), vplayerid
    from dws_jordass_mode_roundrecord_di
    where dtstatdate >= '20230804' and dtstatdate <= date_add('20230804',6)
      and modename='组队竞技' and submodename like '%消灭战模式%'
    group by vplayerid

    union all
    select '幻想混战', min(dtstatdate), vplayerid
    from dws_jordass_mode_roundrecord_di
    where dtstatdate >= '20241115' and dtstatdate <= date_add('20241115',6)
      and modename='创意创作间' and submodename='幻想混战'
    group by vplayerid

    union all
    select '荒野传说', min(dtstatdate), vplayerid
    from dws_jordass_mode_roundrecord_di
    where dtstatdate >= '20240903' and dtstatdate <= date_add('20240903',6)
      and modename='休闲模式' and submodename in ('荒野传说','荒野沙漠')
    group by vplayerid

    union all
    select '策略载具', min(dtstatdate), vplayerid
    from dws_jordass_mode_roundrecord_di
    where dtstatdate >= '20241010' and dtstatdate <= date_add('20241010',6)
      and modename='休闲模式' and submodename like '%策略载具%'
    group by vplayerid

    union all
    select '炎夏混战', min(dtstatdate), vplayerid
    from dws_jordass_mode_roundrecord_di
    where dtstatdate >= '20240625' and dtstatdate <= date_add('20240625',6)
      and modename='创意创作间' and submodename like '%炎夏混战%'
    group by vplayerid

    union all
    select '单人装备', min(dtstatdate), vplayerid
    from dws_jordass_mode_roundrecord_di
    where dtstatdate >= '20240517' and dtstatdate <= date_add('20240517',6)
      and modename='组队竞技' and submodename like '%单人装备%'
    group by vplayerid

    union all
    select '交叉堡垒', min(dtstatdate), vplayerid
    from dws_jordass_mode_roundrecord_di
    where dtstatdate >= '20240412' and dtstatdate <= date_add('20240412',6)
      and modename='组队竞技' and submodename like '%交叉堡垒%'
    group by vplayerid
) t
group by itype, vplayerid;


So, the answer to the question "统计各个玩法上线首周留存情况..." is =
Intermediate_representation: 使用两个派生表（a：每玩法每玩家首日；b：每玩法每玩家在首周后第0~13天的行为记录），然后按玩法、首日和 datediff 关联统计 day 维度留存人数。
SQL:

select  a.itype,
        a.dtstatdate,
        datediff(b.dtstatdate,a.dtstatdate) as idaynum,
        count(distinct a.vplayerid)           as iusernum
from (                      
    select
        itype,
        min(dtstatdate) as dtstatdate,
        vplayerid
    from  (
        select '广域战场'      as itype,
                min(dtstatdate) as dtstatdate,
                vplayerid
        from dws_jordass_mode_roundrecord_di
        where dtstatdate >= '20240723' and dtstatdate <= date_add('20240723',6)
        and submodename = '广域战场模式'
        group by vplayerid

        union all
        select '消灭战', min(dtstatdate), vplayerid
        from dws_jordass_mode_roundrecord_di
        where dtstatdate >= '20230804' and dtstatdate <= date_add('20230804',6)
        and modename='组队竞技' and submodename like '%消灭战模式%'
        group by vplayerid

        union all
        select '幻想混战', min(dtstatdate), vplayerid
        from dws_jordass_mode_roundrecord_di
        where dtstatdate >= '20241115' and dtstatdate <= date_add('20241115',6)
        and modename='创意创作间' and submodename='幻想混战'
        group by vplayerid

        union all
        select '荒野传说', min(dtstatdate), vplayerid
        from dws_jordass_mode_roundrecord_di
        where dtstatdate >= '20240903' and dtstatdate <= date_add('20240903',6)
        and modename='休闲模式' and submodename in ('荒野传说','荒野沙漠')
        group by vplayerid

        union all
        select '策略载具', min(dtstatdate), vplayerid
        from dws_jordass_mode_roundrecord_di
        where dtstatdate >= '20241010' and dtstatdate <= date_add('20241010',6)
        and modename='休闲模式' and submodename like '%策略载具%'
        group by vplayerid

        union all
        select '炎夏混战', min(dtstatdate), vplayerid
        from dws_jordass_mode_roundrecord_di
        where dtstatdate >= '20240625' and dtstatdate <= date_add('20240625',6)
        and modename='创意创作间' and submodename like '%炎夏混战%'
        group by vplayerid

        union all
        select '单人装备', min(dtstatdate), vplayerid
        from dws_jordass_mode_roundrecord_di
        where dtstatdate >= '20240517' and dtstatdate <= date_add('20240517',6)
        and modename='组队竞技' and submodename like '%单人装备%'
        group by vplayerid

        union all
        select '交叉堡垒', min(dtstatdate), vplayerid
        from dws_jordass_mode_roundrecord_di
        where dtstatdate >= '20240412' and dtstatdate <= date_add('20240412',6)
        and modename='组队竞技' and submodename like '%交叉堡垒%'
        group by vplayerid
    ) t
    group by itype, vplayerid
) a
left join (
        select '广域战场' as itype, dtstatdate, vplayerid
        from dws_jordass_mode_roundrecord_di
        where dtstatdate >= '20240723' and dtstatdate <= date_add('20240723',13)
          and submodename = '广域战场模式'
        group by dtstatdate, vplayerid

        union all
        select '消灭战', dtstatdate, vplayerid
        from dws_jordass_mode_roundrecord_di
        where dtstatdate >= '20230804' and dtstatdate <= date_add('20230804',13)
          and modename='组队竞技' and submodename like '%消灭战模式%'
        group by dtstatdate, vplayerid

        union all
        select '幻想混战', dtstatdate, vplayerid
        from dws_jordass_mode_roundrecord_di
        where dtstatdate >= '20241115' and dtstatdate <= date_add('20241115',13)
          and modename='创意创作间' and submodename='幻想混战'
        group by dtstatdate, vplayerid

        union all
        select '荒野传说', dtstatdate, vplayerid
        from dws_jordass_mode_roundrecord_di
        where dtstatdate >= '20240903' and dtstatdate <= date_add('20240903',13)
          and modename='休闲模式' and submodename in ('荒野传说','荒野沙漠')
        group by dtstatdate, vplayerid

        union all
        select '策略载具', dtstatdate, vplayerid
        from dws_jordass_mode_roundrecord_di
        where dtstatdate >= '20241010' and dtstatdate <= date_add('20241010',13)
          and modename='休闲模式' and submodename like '%策略载具%'
        group by dtstatdate, vplayerid

        union all
        select '炎夏混战', dtstatdate, vplayerid
        from dws_jordass_mode_roundrecord_di
        where dtstatdate >= '20240625' and dtstatdate <= date_add('20240625',13)
          and modename='创意创作间' and submodename like '%炎夏混战%'
        group by dtstatdate, vplayerid

        union all
        select '单人装备', dtstatdate, vplayerid
        from dws_jordass_mode_roundrecord_di
        where dtstatdate >= '20240517' and dtstatdate <= date_add('20240517',13)
          and modename='组队竞技' and submodename like '%单人装备%'
        group by dtstatdate, vplayerid

        union all
        select '交叉堡垒', dtstatdate, vplayerid
        from dws_jordass_mode_roundrecord_di
        where dtstatdate >= '20240412' and dtstatdate <= date_add('20240412',13)
          and modename='组队竞技' and submodename like '%交叉堡垒%'
        group by dtstatdate, vplayerid
) b
  on  a.itype      = b.itype
and  a.vplayerid    = b.vplayerid
where datediff(b.dtstatdate,a.dtstatdate) between 0 and 7
group by a.itype, a.dtstatdate, datediff(b.dtstatdate,a.dtstatdate);


Q: "统计2019.5.8至2025.3.30 分月的玩法主玩情况
输出：月份(201905、201906、...、202503)、主玩玩法、主玩人数、总参与人数"
Schema_links: [dws_jordass_mode_roundrecord_di.dtstatdate,dws_jordass_mode_roundrecord_di.vplayerid,dws_jordass_mode_roundrecord_di.roundtime,dws_jordass_mode_roundrecord_di.roundcnt,dwd_jordass_playerexitgamerecord_hi.tdbank_imp_date,dwd_jordass_playerexitgamerecord_hi.mode]
A: Let's think step by step. "统计2019.5.8至2025.3.30 分月的玩法主玩情况" can be solved by knowing the answer to the following sub-question "对于每个玩家在每个月，计算他们在每个玩法的累计时长（或对局数）以确定该玩家的主玩玩法（累计时长最多者）"。
The SQL query for the sub-question "对于每个玩家在每个月，计算他们在每个玩法的累计时长（或对局数）" is:

with main_user as (
    select substr(dtstatdate, 1, 6) mons,
        case
            when modename = '传统模式' and submodename like 'CG%' and mapname = '群屿' then '主题群屿'
            when modename = '传统模式' and mapname = '群屿' then '传统群屿'
            when modename = '传统模式' and mapname = '假日群岛' then '假日群岛'
            when modename = '传统模式' and mapname = '荣耀之城' then '荣耀之城'
            when submodename = '广域战场模式' then '广域战场'
            when submodename = '极能形态模式' then '极能形态'
            when modename = '组队竞技' then '组竞'
            when modename = '乐园' then '乐园'
            when modename = '领地' then '领地'
            when modename = '广阔天地' then '广阔天地'
            else '其他模式'
        end imodename,
        vplayerid,
        sum(roundtime) / 60 roundtime,
        sum(roundcnt) roundcnt
   from dws_jordass_mode_roundrecord_di
   where dtstatdate between '20190508' and '20250330'
   group by vplayerid, substr(dtstatdate, 1, 6),
        case
            when modename = '传统模式' and submodename like 'CG%' and mapname = '群屿' then '主题群屿'
            when modename = '传统模式' and mapname = '群屿' then '传统群屿'
            when modename = '传统模式' and mapname = '假日群岛' then '假日群岛'
            when modename = '传统模式' and mapname = '荣耀之城' then '荣耀之城'
            when submodename = '广域战场模式' then '广域战场'
            when submodename = '极能形态模式' then '极能形态'
            when modename = '组队竞技' then '组竞'
            when modename = '乐园' then '乐园'
            when modename = '领地' then '领地'
            when modename = '广阔天地' then '广阔天地'
            else '其他模式'
        end

   union all

   select substr (tdbank_imp_date, 1, 6) mons,
        '隧道' as modename,
        vplayerid,
        sum(roundtime) / 60 roundtime,
        count(distinct gameid) roundcnt
   from dwd_jordass_playerexitgamerecord_hi
   where tdbank_imp_date between '2024061800' and '2025033023'
     and mode in (3001, 3002, 3003)
   group by substr (tdbank_imp_date, 1, 6), vplayerid
)


So, the answer to the question "统计2019.5.8至2025.3.30 分月的玩法主玩情况" is =
Intermediate_representation: 用 CTE 汇总每月每玩家每玩法的累计时长 / 对局数，按每个月每玩家取 roundtime 最大的玩法为主玩，再统计主玩人数与总参与人数。
SQL:

with main_user as (
    select substr(dtstatdate, 1, 6) mons,
        case
            when modename = '传统模式' and submodename like 'CG%' and mapname = '群屿' then '主题群屿'
            when modename = '传统模式' and mapname = '群屿' then '传统群屿'
            when modename = '传统模式' and mapname = '假日群岛' then '假日群岛'
            when modename = '传统模式' and mapname = '荣耀之城' then '荣耀之城'
            when submodename = '广域战场模式' then '广域战场'
            when submodename = '极能形态模式' then '极能形态'
            when modename = '组队竞技' then '组竞'
            when modename = '乐园' then '乐园'
            when modename = '领地' then '领地'
            when modename = '广阔天地' then '广阔天地'
            else '其他模式'
        end imodename,
        vplayerid,
        sum(roundtime) / 60 roundtime,
        sum(roundcnt) roundcnt
   from dws_jordass_mode_roundrecord_di
   where dtstatdate between '20190508' and '20250330'
   group by vplayerid, substr(dtstatdate, 1, 6),
        case
            when modename = '传统模式' and submodename like 'CG%' and mapname = '群屿' then '主题群屿'
            when modename = '传统模式' and mapname = '群屿' then '传统群屿'
            when modename = '传统模式' and mapname = '假日群岛' then '假日群岛'
            when modename = '传统模式' and mapname = '荣耀之城' then '荣耀之城'
            when submodename = '广域战场模式' then '广域战场'
            when submodename = '极能形态模式' then '极能形态'
            when modename = '组队竞技' then '组竞'
            when modename = '乐园' then '乐园'
            when modename = '领地' then '领地'
            when modename = '广阔天地' then '广阔天地'
            else '其他模式'
        end

   union all

   select substr (tdbank_imp_date, 1, 6) mons,
        '隧道' as modename,
        vplayerid,
        sum(roundtime) / 60 roundtime,
        count(distinct gameid) roundcnt
   from dwd_jordass_playerexitgamerecord_hi
   where tdbank_imp_date between '2024061800' and '2025033023'
     and mode in (3001, 3002, 3003)
   group by substr (tdbank_imp_date, 1, 6), vplayerid
)

select a.mons,
       a.imodename,
       a.iusernum,
       b.iusernumall
from (
    select mons,
          imodename,
          count(vplayerid) iusernum
    from (
        select mons, vplayerid, imodename
        from (
            select mons,
                vplayerid,
                imodename,
                roundtime,
                row_number() over (partition by mons, vplayerid order by roundtime desc) as rn
            from main_user
        )ff1
        where rn = 1
    )f1
   group by mons, imodename
) a
left join (
    select mons,
        imodename,
        count(vplayerid) iusernumall
   from main_user
   group by mons, imodename
) b 
on a.imodename = b.imodename and a.mons = b.mons
order by a.mons, a.imodename
;


Q: "统计2024年12月14日-2024年12月20日参与建筑争夺玩法的玩家人数（去重）有多少，其中多少人是12月13日及以前有参与过建筑争夺的老玩家，有多少是新玩家
输出: 玩家人数、老玩家人数、新玩家人数"
Schema_links: [dws_jordass_matchlog_stat_di.dtstatdate,dws_jordass_matchlog_stat_di.vplayerid,dws_jordass_playermatchrecord_stat_df.dtstatdate,dws_jordass_playermatchrecord_stat_df.vplayerid]
A: Let's think step by step. "统计2024年12月14日-2024年12月20日参与建筑争夺玩法的玩家人数..." can be solved by knowing the answer to the following sub-question "在 2024-12-14 到 2024-12-20 期间，哪些玩家参与了建筑争夺？" 以及 "在 2024-12-13（或更早）有哪些玩家曾参与建筑争夺？"。
The SQL query for the sub-question "在 2024-12-14 到 2024-12-20 期间，哪些玩家参与了建筑争夺？" is:

select vplayerid
from dws_jordass_matchlog_stat_di
where dtstatdate between '20241214' and '20241220'
  and imode = 1344338933661592832
  and platid = 255
group by vplayerid


The SQL query for the sub-question "在 2024-12-13（或更早）有哪些玩家曾参与建筑争夺？" is:

select vplayerid
from dws_jordass_playermatchrecord_stat_df
where dtstatdate = '20241213'
  and imode = 1344338933661592832
  and platid = 255
group by vplayerid


So, the answer to the question "统计2024年12月14日-2024年12月20日参与建筑争夺玩法的玩家人数..." is =
Intermediate_representation: select distinct players in target window left join players in prior-date window and count total / existing / new.
SQL:

select
    count(distinct a.vplayerid) as total_players,
    count(distinct case when b.vplayerid is not null then a.vplayerid end) as existing_players,
    count(distinct case when b.vplayerid is null then a.vplayerid end) as new_players
from (
    select vplayerid
    from dws_jordass_matchlog_stat_di
    where dtstatdate between '20241214' and '20241220'
    and imode = 1344338933661592832
    and platid = 255
    group by vplayerid
) a
left join (
    select vplayerid
    from dws_jordass_playermatchrecord_stat_df
    where dtstatdate = '20241213'
    and imode = 1344338933661592832
    and platid = 255
    group by vplayerid
) b
on a.vplayerid = b.vplayerid
;



Q 是问题，A是回答，按照examples 、plan、special_funcitons生成A

Q: {{query}}

schema_links: {{scheme_links}}

A: Let's think step by step. "{{query}}
" can be solved by knowing the answer to the following sub-questions "{{sub_questions}}
".
...
SQL: ```sql
...
```
The SQL query for the sub-question1
(
...
SQL: ```sql
...
```
The SQL query for the sub-question2
)


# 输出
思考之后只输出最终SQL，不要输出其他任何内容：
"""


def _parse_llm_hard(response: dict):
    content = response['content']
    sql = content.split('```sql')[1].split('```')[0]
    return sql

def get_llm_hard_sql(query: str, sub_questions: str, scheme: str, scheme_links: str, knowledge: str, config: dict):
    formatted_prompt = prompt.format(query=query, sub_questions=sub_questions, scheme=scheme, scheme_links=scheme_links, knowledge=knowledge)
    response = call_sse(config['app_key'], '', formatted_prompt)
    sql = _parse_llm_hard(response)
    return sql