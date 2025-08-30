from helper import sign_in, get_datafields, get_standard_search_scope, create_simulation_data

sess = sign_in()

searchScope = get_standard_search_scope()
dataField = get_datafields(s=sess, searchScope=searchScope, dataset_id='news12') # 这里可以改改
dataField = dataField[dataField['type'] == "MATRIX"]
dataField.head()

datafields_list_dataField = dataField['id'].values
print(datafields_list_dataField)
print(len(datafields_list_dataField))


# 将datafield替换到Alpha模板(框架)中group_rank({fundamental model data}/cap,subindustry)批量生成Alpha
alpha_list = []

for index,datafield in enumerate(datafields_list_dataField,start=1):
    
    alpha_expression = f'group_rank(({datafield})/cap, subindustry)'
    print(f"正在循环第 {index} 个元素,组装alpha表达式: {alpha_expression}")
    simulation_data = create_simulation_data(alpha_expression)
    alpha_list.append(simulation_data)

print(f"there are {len(alpha_list)} Alphas to simulate")
print(alpha_list[0])


# 将Alpha一个一个发送至服务器进行回测,并检查是否断线，如断线则重连，并继续发送
from time import sleep

for index,alpha in enumerate(alpha_list,start=1):
    if index < 1:   #如果中断重跑，可以修改1从指定位置重跑，即可跳过已经模拟过的Alpha
        continue
    if index % 100 == 0:
        sess = sign_in()
        print(f"重新登录，当前index为{index}")
        
    sim_resp = sess.post(
        'https://api.worldquantbrain.com/simulations',
        json=alpha,
    )

    try:
        sim_progress_url = sim_resp.headers['Location']
        while True:
            sim_progress_resp = sess.get(sim_progress_url)
            retry_after_sec = float(sim_progress_resp.headers.get("Retry-After", 0))
            if retry_after_sec == 0:  # simulation done!模拟完成!
                break
            sleep(retry_after_sec)
        alpha_id = sim_progress_resp.json()["alpha"]  # the final simulation result.# 最终模拟结果
        print(f"{index}: {alpha_id}: {alpha['regular']}")
    except:
        print("no location, sleep for 10 seconds and try next alpha.“没有位置，睡10秒然后尝试下一个字母。”")
        sleep(10)

