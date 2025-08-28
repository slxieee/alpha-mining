# 登录
from helper import sign_in, get_datafields, get_standard_search_scope, create_simulation_data, generate_alpha_combinations

sess = sign_in()

searchScope = get_standard_search_scope()
fnd6 = get_datafields(s=sess, searchScope=searchScope, dataset_id='fundamental2')
fnd6 = fnd6[fnd6['type'] == "MATRIX"] # 筛选类型为 "MATRIX" 的数据字段
datafields_list_fnd6 = fnd6['id'].values  # 提取数据字段的ID并转换为列表 比如：['assets', 'liabilities', 'revenue', ...]
print(datafields_list_fnd6)
print(len(datafields_list_fnd6))

# 定义分组比较操作符
group_compare_op = ['group_rank', 'group_zscore', 'group_neutralize']  # 分组比较操作符列表
# 定义时间序列比较操作符
ts_compare_op = ['ts_rank', 'ts_zscore', 'ts_av_diff']  # 时间序列比较操作符列表
# 定义公司基本面数据的字段列表
company_fundamentals = datafields_list_fnd6
# 定义时间周期列表
days = [60, 200]
# 定义分组依据列表
group = ['market', 'industry', 'subindustry', 'sector', 'densify(pv13_h_f1_sector)']

# 使用helper函数生成Alpha表达式组合
alpha_expressions = generate_alpha_combinations(
    group_compare_op, 
    ts_compare_op, 
    company_fundamentals, 
    days, 
    group
)

# 输出生成的alpha表达式总数 # 打印或返回结果字符串列表
print(f"there are total {len(alpha_expressions)} alpha expressions")

# 打印结果
print(alpha_expressions[:5])
print(len(alpha_expressions))

# 将datafield替换到Alpha模板(框架)中group_rank({fundamental model data}/cap,subindustry)批量生成Alpha
alpha_list = []

print("将alpha表达式与setting封装")
for index, alpha_expression in enumerate(alpha_expressions, start=1):
    print(f"正在循环第 {index} 个元素,组装alpha表达式: {alpha_expression}")
    simulation_data = create_simulation_data(alpha_expression)
    alpha_list.append(simulation_data)
print(f"there are {len(alpha_list)} Alphas to simulate")

# 输出
print(alpha_list[0])

# 将Alpha一个一个发送至服务器进行回测,并检查是否断线，如断线则重连
##设置log
import logging
# Configure the logging setting
logging.basicConfig(filename='simulation.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')



from time import sleep
import logging

alpha_fail_attempt_tolerance = 15 # 每个alpha允许的最大失败尝试次数

# 从第0个元素开始迭代回测alpha_list
for index in range(0, len(alpha_list)):
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
            keep_trying = False  # 成功获取位置，退出while循环

        except Exception as e:
            # 处理异常：记录错误，让程序休眠15秒后重试
            logging.error(f"No Location, sleep 15 and retry, error message: {str(e)}")
            print("No Location, sleep 15 and retry")
            sleep(15)  # 休眠15秒后重试
            failure_count += 1  # 增加失败尝试次数

            # 检查失败尝试次数是否达到容忍上限
            if failure_count >= alpha_fail_attempt_tolerance:
                sess = sign_in()  # 重新登录会话
                failure_count = 0  # 重置失败尝试次数
                logging.error(f"No location for too many times, move to next alpha {alpha['regular']}")  # 记录错误
                print(f"No location for too many times, move to next alpha {alpha['regular']}")  # 打印信息
                break  # 退出while循环，移动到for循环中的下一个alpha