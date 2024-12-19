import requests
import functools
import time
import re
import json
from openai import OpenAI
import tiktoken
from config import Config
zhipu_key = Config.api_key
client = OpenAI(
    api_key=zhipu_key,
    base_url="https://open.bigmodel.cn/api/paas/v4/"
)

enc = tiktoken.encoding_for_model("gpt-4")


def try_n_times(n):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(n):
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    if i == n - 1:  # 当达到最大尝试次数时，抛出异常
                        raise e
                    print(f"Attempt {i + 1} failed. Retrying...")
                    time.sleep(10)  # 可选，等待1秒后重试

        return wrapper

    return decorator


def super_eval(json_str, try_num=0):
    if try_num > 3:
        return 'json格式错误'
    json_str = json_str.replace('：', ':')
    try:
        all_json = re.findall('```json(.*?)```', json_str, re.DOTALL)
        if all_json:
            try:
                return eval(all_json[-1])
            except:

                return json.loads(all_json[-1])
        if '```json' in json_str:
            json_str = json_str.replace('```json', '')
        json_str = json_str.replace('```', '')
        try:
            return eval(json_str)
        except:
            return json.loads(json_str)
    except:
        text = llm(f"输出以下内容的json部分并修复成正确格式备注仅仅输出最后的json:```{json_str}```")
        try_num += 1
        return super_eval(text, try_num)


@try_n_times(3)
def llm_eval(content, check_function=None):
    if check_function:
        llm_res = llm(content)

        res = super_eval(llm_res)
        if check_function(json.dumps(res)):
            return res
        else:
            messages = [{'role': 'user', 'content': content},
                        {'role': 'assistant', 'content': llm_res},
                        {'role': 'user',
                         'content': 'json格式化检查未通过，请严格按照json格式输出,并且将json内容放在```json\n```内'}
                        ]
            llm_res = llm(messages)
            res = super_eval(llm_res)
            if check_function(json.dumps(res)):
                return res
            else:
                raise ValueError
    else:
        return super_eval(llm(content))


@try_n_times(3)
def llm(content, print_str=True, max_input=70000, max_rounds=25):
    if isinstance(content, str):
        messages = [{'role': 'user', 'content': content}]
    elif isinstance(content, list):
        if len(content) > max_rounds:
            raise ValueError('对话轮数过长')
        messages = content
    else:
        raise ValueError('输入格式错误')

    if len(enc.encode(str(messages))) > max_input:
        raise ValueError('消息过长')

    completion = client.chat.completions.create(
        # model="GLM-4-AirX",
        model="GLM-4-plus",
        messages=messages,
        top_p=0.7,
        temperature=0.9,
        max_tokens=5000,
        stream=False,
        # timeout=10,
        tools=[{'type': 'web_search', 'web_search': {"enable": False, 'type': 'web_search'}}],
    )

    answer = completion.choices[0].message.content
    if print_str:
        print(answer)

    if isinstance(content, list):
        content.append({"role": "assistant", "content": answer})

    return answer


def llm_lite(content):
    if isinstance(content, str):
        messages = [{'role': 'user', 'content': content}]
    elif isinstance(content, list):
        messages = content
    else:
        raise ValueError
    completion = client.chat.completions.create(
        # model="GLM-4-Flash",
        model="GLM-4-AirX",
        messages=messages,
        top_p=0.7,
        temperature=0.9
    )

    answer = completion.choices[0].message.content
    if isinstance(content, list):
        content.append({"role": "assistant", "content": answer})

    return answer


def Embedding(x):
    if isinstance(x, str):
        return client.embeddings.create(input=x, model='Embedding-3').data[0].embedding
    if isinstance(x, list):
        embeddings = client.embeddings.create(input=x, model='Embedding-3').data
        return [i.embedding for i in embeddings]

if __name__ == '__main__':
    llm([''])