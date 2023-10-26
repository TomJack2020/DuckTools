# -*- encoding: utf-8 -*-
'''
@File    :   duck_tools.py
@Time    :   2023/10/19 10:38:09
@Author  :   cep 
'''
import duckdb
import queue
from tqdm import tqdm
import threading


class DuckDbTools:
    def __init__(self) -> None:
        self.db_path = "data01"
        self.read_only = False
        self.queue_num = 5

    def con_duck(self):
        c = duckdb.connect(self.db_path, read_only=self.read_only)
        # c.sql("show tables").show()
        return c


class DownData(DuckDbTools):
    def __init__(self) -> None:
        super().__init__()

    def get_data_down(self, task_list, func_do):
        # 创建队列，队列的最大个数及限制线程个数
        q = queue.Queue(maxsize=30)
        # 测试数据，多线程查询数据库
        list_bar = tqdm(task_list)
        for tid in list_bar:
            # list_bar.set_description("Processing")
            # 创建线程并放入队列中
            t = threading.Thread(target=func_do, args=(tid,))
            q.put(t)
            # 队列队满
            if q.qsize() == self.queue_num:
                # 用于记录线程，便于终止线程
                join_thread = []
                # 从对列取出线程并开始线程，直到队列为空
                while not q.empty():
                    t = q.get()
                    join_thread.append(t)
                    t.start()
                # 终止上一次队满时里面的所有线程
                for t in join_thread:
                    t.join()
