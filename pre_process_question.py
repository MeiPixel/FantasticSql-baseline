from llm import llm,super_eval
import json
import requests

def exec_sql_s(sql):
    headers = {
        "Authorization": "Bearer 000",
        "Accept": "application/json"
    }
    url = "https://comm.chatglm.cn/finglm2/api/query"

    response = requests.post(url, headers=headers, json={
        "sql": sql,
        "limit": 100
    })
    print(response.text)
    if 'data' not in response.json():
        print(response.json())
    return response.json()['data']


def output_result(result, info_list):
    if 'data' in result and len(result['data']) > 0:
        info_list.append(json.dumps(result['data'], ensure_ascii=False, indent=1) + '\n')

    return info_list


def process_company_name(value):
    res_lst = []
    # 处理公司名称，在三个表中搜索匹配的记录
    tables = ['ConstantDB.SecuMain', 'ConstantDB.HK_SecuMain', 'ConstantDB.US_SecuMain']
    columns_to_match = ['CompanyCode', 'SecuCode', 'ChiName', 'ChiNameAbbr',
                        'EngName', 'EngNameAbbr', 'SecuAbbr', 'ChiSpelling']
    columns_to_select = ['InnerCode', 'CompanyCode', 'SecuCode', 'ChiName', 'ChiNameAbbr',
                         'EngName', 'EngNameAbbr', 'SecuAbbr', 'ChiSpelling']

    value = value.replace("'", "''")  # 防止 SQL 注入

    for table in tables:
        if 'US' in table:
            columns_to_match.remove('ChiNameAbbr')
            columns_to_select.remove('ChiNameAbbr')
            columns_to_match.remove('EngNameAbbr')
            columns_to_select.remove('EngNameAbbr')

        match_conditions = [f"{col} = '{value}'" for col in columns_to_match]
        where_clause = ' OR '.join(match_conditions)
        sql = f"""
        SELECT {', '.join(columns_to_select)}
        FROM {table}
        WHERE {where_clause}
        """
        result = exec_sql_s(sql)
        if result:
            res_lst.append((result, table))
        else:
            continue
    else:
        ...
        # print(f"未在任何表中找到公司名称为 {value} 的信息。")

    return res_lst

def process_code(value):
    res_lst = []
    # 处理代码，在三个表中搜索匹配的记录
    tables = ['ConstantDB.SecuMain', 'ConstantDB.HK_SecuMain', 'ConstantDB.US_SecuMain']
    columns_to_select = ['InnerCode', 'CompanyCode', 'SecuCode', 'ChiName', 'ChiNameAbbr',
                         'EngName', 'EngNameAbbr', 'SecuAbbr', 'ChiSpelling']

    value = value.replace("'", "''")  # 防止 SQL 注入

    for table in tables:
        if 'US' in table:
            columns_to_select.remove('ChiNameAbbr')
            columns_to_select.remove('EngNameAbbr')

        sql = f"""
        SELECT {', '.join(columns_to_select)}
        FROM {table}
        WHERE SecuCode = '{value}'
        """
        result = exec_sql_s(sql)
        if result:
            res_lst.append((result, table))
        else:
            continue
    else:
        ...
        # print(f"未在任何表中找到代码为 {value} 的信息。")
    return res_lst

def process_items(item_list):
    res_list = []
    for item in item_list:
        key, value = list(item.items())[0]
        if key == "基金名称":
            res_list.extend(process_company_name(value))
        elif key == "公司名称":
            res_list.extend(process_company_name(value))
        elif key == "代码":
            res_list.extend(process_code(value))
        else:
            print(f"无法识别的键：{key}")
    res_list = [i for i in res_list if i]
    res = ''
    tables = []
    for i, j in res_list:
        tables.append(j)
        res += f"预处理程序通过表格：{j} 查询到以下内容：\n {json.dumps(i, ensure_ascii=False, indent=1)} \n"
    return res,tables


def process_question(question):
    prompt = '''
你将会进行命名实体识别任务，并输出实体json

你只需要识别以下三种实体：
-公司名称
-代码
-基金名称。

只有出现了才识别，问题不需要回答

其中，公司名称可以是全称，简称，拼音缩写
代码包含股票代码和基金代码
基金名称包含债券型基金，

以下是几个示例：
user:实体识别任务：```唐山港集团股份有限公司是什么时间上市的（回答XXXX-XX-XX）
当年一共上市了多少家企业？
这些企业有多少是在北京注册的？```
assistant:```json
[{"公司名称":"唐山港集团股份有限公司"}]
```
user:实体识别任务：```JD的职工总数有多少人？
该公司披露的硕士或研究生学历（及以上）的有多少人？
20201月1日至年底退休了多少人？```
assistant:```json
[{"公司名称":"JD"}]
```
user:实体识别任务：```600872的全称、A股简称、法人、法律顾问、会计师事务所及董秘是？
该公司实控人是否发生改变？如果发生变化，什么时候变成了谁？是哪国人？是否有永久境外居留权？（回答时间用XXXX-XX-XX）```
assistant:```json
[{"代码":"600872"}]
```
user:实体识别任务：```华夏鼎康债券A在2019年的分红次数是多少？每次分红的派现比例是多少？
基于上述分红数据，在2019年最后一次分红时，如果一位投资者持有1000份该基金，税后可以获得多少分红收益？```
assistant:```json
[{"基金名称":"华夏鼎康债券A"}]
```
user:实体识别任务：```化工纳入过多少个子类概念？```
assistant:```json
[]
```
'''

    messages = [{'role': 'system', 'content': prompt},

                {'role': 'user', 'content':f'''实体识别任务：```{question}```''' }]

    a1 = llm(messages)
    a2 = super_eval(a1)
    return process_items(a2)


if __name__ == '__main__':
    question = '工商银行的H股代码、中文名称及英文名称分别是？该公司的主席及公司邮箱是？该公司2020年12月底披露的变更前后的员工人数为多少人？'
    res = process_question(question)

    print(res)

