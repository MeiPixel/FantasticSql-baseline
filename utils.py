import requests
import re
import json
from llm import super_eval, llm
from fuzzywuzzy import fuzz
import pandas as pd
import numpy as np


def exec_sql(sql_message):
    sql = re.findall('```sql(.*?)```', sql_message, re.DOTALL)[-1]
    headers = {
        "Authorization": "Bearer 000",
        "Accept": "application/json"
    }
    url = "https://comm.chatglm.cn/finglm2/api/query"
    sql = '\n'.join([i for i in sql.split('\n') if not i.strip().startswith('-')])
    response = requests.post(url, headers=headers, json={
        "sql": sql,
        "limit": 100
    })
    print(response.text)
    return json.dumps(response.json(), indent=2, ensure_ascii=False), sql


data = pd.read_excel('./data/数据字典.xlsx')
table_name2db_name = dict(zip(data['表英文'], data['库名英文']))
data = data.to_dict(orient='records')
table_name2desc = {}
for i in data:
    s = ''
    for j, k in i.items():
        s += f"{j}:{k}\n"

    table_name2desc[i['表英文']] = s

# Group the dataframe by 'table_name' and then convert each group to a list of dictionaries
table2column = pd.read_excel('./data/数据字典.xlsx', sheet_name='表字段信息')
grouped_dict = {
    table_name: group.drop('table_name', axis=1).to_dict(orient='records')
    for table_name, group in table2column.groupby('table_name')
}
for k, v in grouped_dict.items():
    vv = []
    for i in v:
        i = {p: q for p, q in i.items() if isinstance(q, str) and p != 'Annotation'}

        vv.append(i)
    grouped_dict[k] = vv


def get_table_desc(table_name, columns=[], get_sample=False, get_colunm=True, recall_num=30, recall_by=''):
    '''
    根据表格名称获取表格内容
    :param table_name:
    :param columns:
    :param get_sample:
    :param get_colunm:
    :param recall_num:
    :param recall_by:
    :return:
    '''
    if '.' in table_name:
        full_name = table_name
        table_name = table_name.split('.')[-1]
    else:
        full_name = table_name2db_name[table_name] + '.' + table_name
    a1 = table_name2desc[table_name]
    if columns:

        pos_columns = [i['column_name'] for i in grouped_dict[table_name] if \
                       i['column_name'] in columns or re.search('id|code|day$', i['column_name'], re.IGNORECASE)]
        a2 = json.dumps(
            [i for i in grouped_dict[table_name] if \
             i['column_name'] in pos_columns],
            ensure_ascii=False, indent=1)
    else:
        if recall_by and len(grouped_dict[table_name]) > recall_num:
            column_names = [i['column_description'] for i in grouped_dict[table_name]]
            sim = [fuzz.partial_ratio(i, recall_by) for i in column_names]
            sim_index = np.argsort(sim)[::-1][:recall_num]

            pos_columns = [i['column_name'] for i in grouped_dict[table_name] if
                           re.search('id|code|day$', i['column_name'], re.IGNORECASE)]

            for inx in sim_index:
                pos_columns.append(grouped_dict[table_name][inx]['column_name'])

            a2 = json.dumps(
                [i for i in grouped_dict[table_name] if \
                 i['column_name'] in pos_columns],
                ensure_ascii=False, indent=1)

        else:
            a2 = json.dumps(grouped_dict[table_name], ensure_ascii=False, indent=1)

    res = f'表格名称:{full_name}\n描述:{a1}'

    if get_colunm:
        res += f'\n字段描述:{a2}'

    if get_sample:
        if columns:
            a3 = str(pd.read_csv(f'./database/{full_name}.csv', encoding='utf8')[pos_columns].head(5))
        else:
            a3 = str(pd.read_csv(f'./database/{full_name}.csv', encoding='utf8').head(5))
        res += f'\n示例:{a3}'

    return res


data = pd.read_excel('./data/数据字典.xlsx')

data_list = data.to_dict(orient='records')

table_content = ''
for i in data_list:
    table_content += str(i)
    table_content += '\n---\n'


def recall_table(question_content, tables_desc=table_content):
    '''
    召回表格
    :param question_content:
    :param tables_desc:
    :return:
    '''
    messages = [{'role': 'system', 'content': f'''
数据表说明如下：
{tables_desc}
用户会给你数据表描述和问题串，请针对每个问题串仔细分析其中的每个问题需要用哪些表格查询，可以是一个或者多个。'''},
                {'role': 'user', 'content': f"""问题串为:`{question_content}`""" + """
使用以下格式回答问题：
```json
[
{"question":"针对的question1","query_requirements":"针对问题里的哪些查询需求","table_name":"表格名称"},
{"question":"针对的question1","query_requirements":"针对问题里的哪些查询需求","table_name":"表格名称"},
{"question":"针对的question2","query_requirements":"针对问题里的哪些查询需求","table_name":"表格名称"},
...
]
```
备注，针对同一个问题，可以有多条表数据。
请区分港股美股A股的数据在对应的表格内。
table_name只多不少，尽可能列举全，且为`表英文`字段。
"""}]

    min_table = super_eval(llm(messages))
    return min_table


def innercode2name(innercode):
    '''
    答案替换 innercode
    :param innercode:
    :return:
    '''
    tables = ['ConstantDB.SecuMain', 'ConstantDB.HK_SecuMain']
    for table in tables:
        sql = f'''```sql
select ChiName,ChiNameAbbr from {table} where InnerCode={innercode}
```'''
        resp, sql = exec_sql(sql)
        res = json.loads(resp)
        if res['data']:
            s = ''
            for i in res['data']:
                s += f"{i['ChiName']}({i['ChiNameAbbr']})"
            return s


def CompanyCode2name(CompanyCode):
    tables = ['ConstantDB.SecuMain', 'ConstantDB.HK_SecuMain']
    for table in tables:
        sql = f'''```sql
    select ChiName,ChiNameAbbr from {table} where CompanyCode={CompanyCode}
    ```'''
        resp, sql = exec_sql(sql)
        res = json.loads(resp)
        if res['data']:
            s = ''
            for i in res['data']:
                s += f"{i['ChiName']}({i['ChiNameAbbr']})"
            return s


def process_answer(answer):
    if 'InnerCode' in answer:
        InnerCode = re.findall('InnerCode[:：]\s?(\d+)', answer)
        for i in InnerCode:
            s = innercode2name(i)
            if s:
                answer = re.sub(r'(?<!\d){}(?!\d)'.format(re.escape(i)), s, answer)
    if 'CompanyCode' in answer:
        CompanyCode = re.findall('CompanyCode[:：]\s?(\d+)', answer)
        for i in CompanyCode:
            s = CompanyCode2name(i)
            if s:
                answer = re.sub(r'(?<!\d){}(?!\d)'.format(re.escape(i)), s, answer)
    answer = re.sub(r'(?<=\d),(?=\d{3}[^0-9])', '', answer)
    return answer


if __name__ == '__main__':
    answer = recall_table(
        "工商银行的H股代码、中文名称及英文名称分别是？\n该公司的主席及公司邮箱是？\n该公司2020年12月底披露的变更前后的员工人数为多少人？", )
    print(answer)
