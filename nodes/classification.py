from matplotlib.pylab import number
from utils import call_sse
import json
from pathlib import Path

prompt = f"""

# For the given question, classify it as NESTED.
Always output sub-questions in the format: sub-questions needed = ["sub-question1", "sub-question2", ...]
Always output Label: "NESTED".


# fields
{{scheme}}

# examples
{{examples}}


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

def get_classification(query: str, scheme: str, scheme_links: str, goldensql_ids:list[number],config: dict):
    templates_path = Path(__file__).parent.parent / 'goldensql_template_query2sub.json'
    examples_text = ""
    try:
        templates = json.loads(templates_path.read_text(encoding='utf-8'))
        # 保证顺序 
        # 根据goldensql_ids[x,y,z]选择对应的模板 sqlx,sqly,sqlz
        selected_keys = [f'sql{sql_id}' for sql_id in goldensql_ids]
        parts = []
        for k in selected_keys:
            if k in templates:
                parts.append(templates[k])
        examples_text = "\n\n".join(parts)
    except Exception:
        examples_text = ""
    # print(f"examples_text: {examples_text}")
    formatted_prompt = prompt.format(
        query=query,
        scheme=scheme,
        scheme_links=scheme_links,
        examples=examples_text
    )
    response = call_sse(config['app_key'], '', formatted_prompt)
    classification_result = _parse_classification(response)
    return classification_result