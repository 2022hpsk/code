from utils.util import call_sse

prompt = f"""
# Task
你是一个SQL专家，需要检查并修复给定SQL语句中的语法或拼写错误，或者可能存在其它冗余或不正确的部分，确保最终SQL是最合适的，并能够在MySQL中成功执行。

# Common knowledge
这是一些背景信息
{{common_knowledge}}

# Question knowledge
这是一些与当前问题相关的信息
{{question_knowledge}}

# Question
{{question}}

# Sub-questions
{{sub_questions}}

# Schema
{{scheme}}

# Schema links
{{scheme_links}}

# Failed SQL
{{failed_sql}}

# Error message
{{error_message}}

# Output format
Error Message仅供你参考，你要先全面分析SQL可能存在的问题，再提供修正后的SQL。输出格式必须如下：
Analysis: <你的分析>
Corrected SQL:
```sql
<修正后的SQL>
```
"""


def _parse_rewriter(response: dict) -> str:
    content = response["content"]
    if "```sql" not in content:
        raise ValueError("响应中未找到SQL代码块")
    sql = content.split("```sql", 1)[1].split("```", 1)[0]
    return sql.strip()


def rewrite_failed_sql(
    sub_questions: str,
    scheme: str,
    scheme_links: str,
    failed_sql: str,
    error_message: str,
    data: dict,
    config: dict,
) -> str:
    try:
        formatted_prompt = prompt.format(
            common_knowledge=config["common_knowledge"],
            question_knowledge=data["knowledge"],
            question=data["question"],
            sub_questions=sub_questions,
            scheme=scheme,
            scheme_links=scheme_links,
            failed_sql=failed_sql,
            error_message=error_message,
        )

        print(formatted_prompt)

        response = call_sse(config["sse"]["app_key"], "", formatted_prompt)
        
        sql = _parse_rewriter(response)
        
        return sql
    except Exception as e:
        raise Exception("Rewrite SQL failed for current sql") from e

