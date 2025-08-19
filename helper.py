"""
WorldQuant Brain API Helper Functions
共用的辅助函数模块
"""

import requests
import json
from os.path import expanduser
from requests.auth import HTTPBasicAuth
import pandas as pd


def sign_in():
    """
    登录WorldQuant Brain API
    
    Returns:
        requests.Session: 已认证的会话对象
    """
    with open(expanduser('brain_credentials.txt')) as f:
        credentials = json.load(f)
    username, password = credentials
    sess = requests.Session()
    sess.auth = HTTPBasicAuth(username, password)
    response = sess.post('https://api.worldquantbrain.com/authentication')
    return sess


def sign_in_with_debug():
    """
    登录WorldQuant Brain API (带调试信息)
    
    Returns:
        requests.Session: 已认证的会话对象
    """
    with open(expanduser('brain_credentials.txt')) as f:
        credentials = json.load(f)
    username, password = credentials
    sess = requests.Session()
    sess.auth = HTTPBasicAuth(username, password)
    response = sess.post('https://api.worldquantbrain.com/authentication')
    print(response.status_code)
    print(response.json())
    return sess


def get_datafields(s, searchScope, dataset_id: str = '', search: str = ''):
    """
    获取数据集中的数据字段
    
    Args:
        s (requests.Session): 已认证的会话对象
        searchScope (dict): 搜索范围配置
        dataset_id (str, optional): 数据集ID. Defaults to ''.
        search (str, optional): 搜索关键词. Defaults to ''.
    
    Returns:
        pandas.DataFrame: 包含数据字段信息的DataFrame
    """
    instrument_type = searchScope['instrumentType']
    region = searchScope['region']
    delay = searchScope['delay']
    universe = searchScope['universe']

    if len(search) == 0:
        url_template = "https://api.worldquantbrain.com/data-fields?" + \
                       f"&instrumentType={instrument_type}" + \
                       f"&region={region}&delay={str(delay)}&universe={universe}&dataset.id={dataset_id}&limit=50" + \
                       "&offset={x}"
        count = s.get(url_template.format(x=0)).json()['count']
    else:
        url_template = "https://api.worldquantbrain.com/data-fields?" + \
                       f"&instrumentType={instrument_type}" + \
                       f"&region={region}&delay={str(delay)}&universe={universe}&limit=50" + \
                       f"&search={search}" + \
                       "&offset={x}"
        count = 100

    datafields_list = []
    for x in range(0, count, 50):
        datafields = s.get(url_template.format(x=x))
        datafields_list.append(datafields.json()['results'])

    datafields_list_flat = [item for sublist in datafields_list for item in sublist]

    datafields_df = pd.DataFrame(datafields_list_flat)
    return datafields_df


def create_simulation_data(alpha_expression, settings=None):
    """
    创建模拟数据配置
    
    Args:
        alpha_expression (str): Alpha表达式
        settings (dict, optional): 自定义设置. Defaults to None.
    
    Returns:
        dict: 模拟数据配置字典
    """
    default_settings = {
        "instrumentType": "EQUITY",
        "region": "USA",
        "universe": "TOP3000",
        "delay": 1,
        "decay": 6,
        "neutralization": "SUBINDUSTRY",
        "truncation": 0.08,
        "pasteurization": "ON",
        "unitHandling": "VERIFY",
        "nanHandling": "ON",
        "language": "FASTEXPR",
        "visualization": False,
    }
    
    if settings:
        default_settings.update(settings)
    
    simulation_data = {
        "type": "REGULAR",
        "settings": default_settings,
        "regular": alpha_expression
    }
    
    return simulation_data


def submit_alpha_simulation(sess, alpha_data):
    """
    提交Alpha模拟并等待结果
    
    Args:
        sess (requests.Session): 已认证的会话对象
        alpha_data (dict): Alpha模拟数据
    
    Returns:
        str: Alpha ID，如果失败返回None
    """
    from time import sleep
    
    try:
        sim_resp = sess.post(
            'https://api.worldquantbrain.com/simulations',
            json=alpha_data,
        )

        sim_progress_url = sim_resp.headers['Location']
        
        while True:
            sim_progress_resp = sess.get(sim_progress_url)
            retry_after_sec = float(sim_progress_resp.headers.get("Retry-After", 0))
            if retry_after_sec == 0:  # simulation done!模拟完成!
                break
            sleep(retry_after_sec)

        alpha_id = sim_progress_resp.json()["alpha"]  # the final simulation result 模拟最终模拟结果
        return alpha_id
    except Exception as e:
        print(f"Error in simulation: {e}")
        return None


def get_standard_search_scope():
    """
    获取标准的搜索范围配置
    
    Returns:
        dict: 标准搜索范围配置
    """
    return {
        'region': 'USA', 
        'delay': '1', 
        'universe': 'TOP3000', 
        'instrumentType': 'EQUITY'
    }


def generate_alpha_combinations(group_compare_ops, ts_compare_ops, company_fundamentals, days_list, groups):
    """
    生成Alpha表达式组合
    
    Args:
        group_compare_ops (list): 分组比较操作符列表
        ts_compare_ops (list): 时间序列比较操作符列表
        company_fundamentals (list): 公司基本面数据字段列表
        days_list (list): 时间周期列表
        groups (list): 分组依据列表
    
    Returns:
        list: Alpha表达式列表
    """
    alpha_expressions = []
    
    for gco in group_compare_ops:
        for tco in ts_compare_ops:
            for cf in company_fundamentals:
                for d in days_list:
                    for grp in groups:
                        alpha_expressions.append(f"{gco}({tco}({cf}, {d}), {grp})")
    
    return alpha_expressions


def batch_submit_alphas(sess, alpha_list, start_index=0, max_failures=15):
    """
    批量提交Alpha进行模拟，带重连和错误处理
    
    Args:
        sess (requests.Session): 已认证的会话对象
        alpha_list (list): Alpha配置列表
        start_index (int): 开始的索引位置
        max_failures (int): 每个Alpha最大失败尝试次数
    
    Returns:
        list: 成功的Alpha ID列表
    """
    import logging
    from time import sleep
    
    successful_alphas = []
    
    # 从指定位置开始迭代回测alpha_list
    for index in range(start_index, len(alpha_list)):
        alpha = alpha_list[index]
        print(f"{index}: {alpha['regular']}")
        logging.info(f"{index}: {alpha['regular']}")
        keep_trying = True  # 控制while循环继续的标志
        failure_count = 0  # 记录失败尝试次数的计数器

        while keep_trying:
            try:
                # 尝试发送POST请求
                sim_resp = sess.post(
                    'https://api.worldquantbrain.com/simulations',
                    json=alpha  # 将当前alpha（一个JSON）发送到服务器
                )

                # 从响应头中获取位置
                sim_progress_url = sim_resp.headers['Location']
                logging.info(f'Alpha location is: {sim_progress_url}')  # 记录位置
                print(f'Alpha location is: {sim_progress_url}')  # 打印位置
                
                # 等待模拟完成
                while True:
                    sim_progress_resp = sess.get(sim_progress_url)
                    retry_after_sec = float(sim_progress_resp.headers.get("Retry-After", 0))
                    if retry_after_sec == 0:  # simulation done!模拟完成!
                        break
                    sleep(retry_after_sec)
                
                alpha_id = sim_progress_resp.json()["alpha"]  # the final simulation result
                successful_alphas.append(alpha_id)
                print(f"Success: {alpha_id}")
                logging.info(f"Success: {alpha_id}")
                
                keep_trying = False  # 成功获取位置，退出while循环

            except Exception as e:
                # 处理异常：记录错误，让程序休眠15秒后重试
                logging.error(f"No Location, sleep 15 and retry, error message: {str(e)}")
                print("No Location, sleep 15 and retry")
                sleep(15)  # 休眠15秒后重试
                failure_count += 1  # 增加失败尝试次数

                # 检查失败尝试次数是否达到容忍上限
                if failure_count >= max_failures:
                    sess = sign_in()  # 重新登录会话
                    failure_count = 0  # 重置失败尝试次数
                    logging.error(f"No location for too many times, move to next alpha {alpha['regular']}")  # 记录错误
                    print(f"No location for too many times, move to next alpha {alpha['regular']}")  # 打印信息
                    break  # 退出while循环，移动到for循环中的下一个alpha
        
        # 每100个Alpha重新登录
        if (index + 1) % 100 == 0:
            sess = sign_in()
            print(f"重新登录，当前index为{index + 1}")
    
    return successful_alphas


def setup_logging(log_filename='simulation.log'):
    """
    设置日志记录
    
    Args:
        log_filename (str): 日志文件名
    """
    import logging
    logging.basicConfig(filename=log_filename, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')


def save_alphas_to_csv(alpha_list, filename='alpha_list_pending_simulated.csv'):
    """
    将Alpha列表保存到CSV文件
    
    Args:
        alpha_list (list): Alpha配置列表
        filename (str): 文件名
    """
    import csv
    import os
    
    # Check if the file exists
    file_exists = os.path.isfile(filename)

    # Write the list of dictionaries to a CSV file
    with open(filename, 'a', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=['type', 'settings', 'regular'])
        # If the file does not exist, write the header
        if not file_exists:
            dict_writer.writeheader()

        dict_writer.writerows(alpha_list)

    print(f"Alpha list has been saved to {filename}")
