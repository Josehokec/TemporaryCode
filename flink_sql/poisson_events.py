"""
代码说明：
lambda_rate 控制事件的平均发生频率。
intervals 使用指数分布（泊松过程的间隔）生成事件间隔时间。
timestamps 使用时间间隔生成时间戳，初始时间为当前时间。
pd.DataFrame 将 id 和 timestamp 组合在一起形成事件数据集。
这样，你就得到了一个简单的模拟事件发生的时间序列数据集。你可以调整 lambda_rate 和 num_events 参数来改变事件的发生频率和总数。


如果某个随机事件在单位时间内发生的次数服从泊松分布，那么该事件连续发生的时间间隔将服从指数分布。‌
"""

import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

def get_average_window_len(lambda_rate,window):
    """
    给定单位时间内产生事件的概率
    算平均窗口长度和窗口ID数量
    """
    # 用多少个事件去模拟
    num_events = 10000
    average_window_len = 0
    repeat_num = 10
    for i in range(repeat_num):
        # 生成事件间隔时间（泊松过程）
        intervals = np.random.exponential(scale=1/lambda_rate, size=num_events)
        # 创建事件时间戳
        start_time = datetime.now()
        timestamps = [start_time]
        for interval in intervals:
            timestamps.append(timestamps[-1] + timedelta(seconds=interval))
        # 将时间戳转换为long类型的Unix时间
        unix_timestamps = [int(ts.timestamp()) for ts in timestamps[1:]]
        #print('unix_timestamps: ', unix_timestamps)

        last_end = -1
        cover_window_len = 0

        # window: [ts, ts + w]
        for ts in unix_timestamps:
            if ts <= last_end:
                overlap_len = last_end - ts
                cover_window_len = cover_window_len + window - overlap_len
                last_end = ts + window
            else:
                cover_window_len = cover_window_len + window
                last_end = ts + window
        average_window_len += cover_window_len / num_events

    average_window_len = average_window_len / repeat_num

    return average_window_len, average_window_len / window 


# 10s一个事件
start_time = time.time()
expected_window_len, expected_window_id_num = get_average_window_len(1, 10)
end_time = time.time()

execution_time = (end_time - start_time) * 1000
print(f"Function execution time: {execution_time:.3f} ms")
print("expected_window_len: ", round(expected_window_len, 3), " expected_window_id_num: ", round(expected_window_id_num, 3))