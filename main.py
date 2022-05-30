# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from pymongo import MongoClient
import networkx as nx
import time
import ScheduleStatus

import matplotlib.pyplot as plt

import http

mongo_client = MongoClient("mongodb://localhost:27017/")
scheduler_db = mongo_client["schedules"]

schedules_collection = scheduler_db["schedules"]
data_collection = scheduler_db["data"]

mongo_client.list_database_names()
scheduler_db.list_collection_names()

client = MongoClient()


class Test():
    def __init__(self, name,age):
        self.name = name
        self.age = age

    def name_age(self):
        return self.name + str(self.age)


G = nx.DiGraph()
G.add_node("ad", obj=Test("supun",21))
G.add_node("b", obj=Test("supunb",20))
G.add_node("c", obj=Test("supunc",2))
G.add_node("node", http_object=Test("supunc",2))


nx.draw_planar(G,
    with_labels=True,
    node_size=1000,
    node_color="#ffff8f",
    width=0.8,
    font_size=14,
)
G.add_edge("ad", "c", test="like")
G.add_edge("ad", "b", test="dislike")
list(nx.edge_dfs(G, 'ad'))
nx.descendants(G, "ad")


def get_pending_tasks():
    pending_tasks = schedules_collection.find({'status': ScheduleStatus.PENDING, "trigger_time": {"lt": time.time() }})
    pending_tasks = [task for task in pending_tasks]
    return pending_tasks


def run():
    start_time = time.perf_counter()
    while time.perf_counter() < start_time + 10:
        pending_tasks = get_pending_tasks()
        for pending_task in pending_tasks:
            required_data = data_collection.find({
                'job_id': pending_task['job_id'],
                "task_id": pending_task['task_id']
            })

            result = G.nodes['node']['http_object'].execute(required_data)

            if result == "success":
                schedules_collection.update({
                    'job_id': pending_task['job_id'],
                    "task_id": pending_task['task_id']
                }, {
                    'status': ScheduleStatus.COMPLETED
                })
            else:
                schedules_collection.update({
                    'job_id': pending_task['job_id'],
                    "task_id": pending_task['task_id']
                }, {
                    'status': ScheduleStatus.FAILED
                })

            downstream_nodes = nx.descendants(G, pending_task['task_id'])




