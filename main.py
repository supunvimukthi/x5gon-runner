import time
import json
import ScheduleStatus

from datetime import datetime

from DAG import X5gon_DAG
from database import create_new_job, update_job_status, update_error, update_retry_count, get_values, get_pending_tasks, \
    insert_values


# limitations: have to define all parameters in request mapping and response mapping
def run():
    start_time = time.perf_counter()
    while True:
        # Run scheduler every 10 second for now
        if time.perf_counter() < (start_time + 10):
            continue

        start_time = time.perf_counter()
        print('running once...')
        # get all pending tasks
        pending_tasks = get_pending_tasks()
        for pending_task in pending_tasks:
            try:
                # get http operator object related to the pending task
                current_task = X5gon_DAG.nodes[pending_task[1]]['http_operator']

                # retrieving data related to the http operator

                required_keys = list(current_task.request_mapping.keys())
                required_data = {}
                if len(required_keys) > 0:
                    required_data = get_values(pending_task[0], required_keys)

                # execute the http request
                result = current_task.execute(required_data)
                if result:
                    # saving data needed for the task from the result dictionary
                    values = []
                    for key in current_task.response_mapping.keys():
                        if key in result:
                            values.append((
                                pending_task[0],
                                key,
                                json.dumps({'value': result[key]}),
                                pending_task[1],
                                datetime.now().timestamp()
                            ))

                    insert_values(values)

                    # accessing all downstream task to this task
                    for next_task_id in X5gon_DAG.successors(pending_task[1]):
                        # scheduling the task
                        create_new_job(pending_task[0], next_task_id)

                    # update the current task as completed
                    update_job_status(pending_task[0], pending_task[1], ScheduleStatus.COMPLETED)
                else:
                    # update status as skipped since operator did not satisfy the condition
                    update_job_status(pending_task[0], pending_task[1], ScheduleStatus.SKIPPED)
            except Exception as e:
                print(e)
                # if retry count is exceeding the max retry count update status as failed
                if pending_task[4] == current_task.max_retry_count:
                    update_error(pending_task[0], pending_task[1], str(e))
                # increase the retry count and keep status as pending to try again
                else:
                    update_retry_count(pending_task[0], pending_task[1], pending_task[4] + 1)
                    update_job_status(pending_task[0], pending_task[1], ScheduleStatus.PENDING)


run()
