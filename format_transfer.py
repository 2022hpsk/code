# 现在我的dataset_exe_result.json的格式是：
# {"sql_id": 1, "sql": "SELECT ...", "status": "success", "result": [...]}
# {xxx}
# {xxx}
# # 但是我要改成一个标准的json数组格式：
# [
#   {"sql_id": 1, "sql": "SELECT ...", "status": "success", "result": [...]},
#   {xxx},
#   {xxx}
# ]

# 读入dataset_exe_result.json文件内容，转换成标准的json数组格式，并保存回dataset_exe_result2.json中
import json
def convert_results_to_json_array(input_file: str, output_file: str):
    results = []
    with open(input_file, 'r') as f:
        for line in f:
            results.append(json.loads(line))
    
    with open(output_file, 'w') as f:
        json.dump(results, f, ensure_ascii=False, indent=4) 
    print(f"Converted results saved to {output_file}")
if __name__ == "__main__":
    convert_results_to_json_array('dataset_exe_result.json', 'dataset_exe_result2.json')
