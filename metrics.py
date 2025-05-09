import datetime
import matplotlib.pyplot as plt
import numpy as np
import requests

# App ID of the Spark job
appid = "app-20250506174016-0000"

format_string = "%Y-%m-%dT%H:%M:%S.%f%Z"

try:
    url = f"http://localhost:18080/api/v1/applications/{appid}/stages"
    response = requests.get(url)
    response.raise_for_status()
    stagedata = response.json()

    num_computed_stages = 0
    num_skipped = 0
    # Job timing
    completion_times = [] # seconds
    scheduling_times = [] # seconds
    exec_run_times = [] # milliseconds
    exec_deserialize_times = [] # milliseconds

    # Disk accesses
    num_disk_access = 0
    disk_bytes_written = []
    disk_write_times = [] # milliseconds

    # Exchange between workers
    num_exchanges = 0
    num_exchanged_bytes = []

    # Failed stage ids
    failed_stages = []

    for stage in stagedata:
        if stage["status"] == "SKIPPED":
            num_skipped = num_skipped + 1
            continue

        stageid = stage["stageId"]
        stage_response = requests.get(f"http://localhost:18080/api/v1/applications/{appid}/stages/{stageid}")
        tasks_json = stage_response.json()[0]
        for task in tasks_json["tasks"]:
            task_json = tasks_json["tasks"][f"{task}"]
            # Calculate executor run time/deserialize time per task in executor
            exec_run_times.append(int(task_json["taskMetrics"]["executorRunTime"]))
            exec_deserialize_times.append(int(task_json["taskMetrics"]["executorDeserializeTime"]))

        num_computed_stages = num_computed_stages + 1
        # Calculate total execution time
        end_time = datetime.datetime.strptime(stage["completionTime"], format_string)
        start_time = datetime.datetime.strptime(stage["submissionTime"], format_string)
        diff = end_time - start_time
        completion_time = diff.seconds + (diff.microseconds / 1000000)
        completion_times.append(completion_time)
        # Calculate scheduler delay time
        first_time = datetime.datetime.strptime(stage["firstTaskLaunchedTime"], format_string)
        diff = first_time - start_time
        scheduling_time = diff.seconds + (diff.microseconds / 1000000)
        scheduling_times.append(scheduling_time)
        # Calculate number of disk accesses
        if int(stage["shuffleWriteBytes"]) > 0:
            num_disk_access = num_disk_access + 1
            disk_bytes_written.append(int(stage["shuffleWriteBytes"]))
            disk_write_times.append( float(stage["shuffleWriteTime"]) / 1000000 )
        # Calculate amount of data exchanged between 
        if int(stage["shuffleRemoteBytesRead"]) > 0:
            num_exchanges = num_exchanges + 1
            num_exchanged_bytes.append(stage["shuffleRemoteBytesRead"])
        # Check failed stages
        if int(stage["numFailedTasks"]) > 0:
            failed_stages.append(stage["stageId"])

    f1 = plt.figure(1)
    plt.title("Spark Job Completion Time CDF")
    sorted_completion_times = np.sort(completion_times)
    completion_time_cdf = np.array([i/sorted_completion_times.size for i in range(sorted_completion_times.size)])
    plt.plot(sorted_completion_times, completion_time_cdf)
    plt.vlines(np.percentile(sorted_completion_times, 99), 0, 0.99, color='red', linestyle='dotted')
    plt.xlabel("Time (seconds)")
    f1.show()
    print("Average Job Completion Time: ", np.average(sorted_completion_times), "seconds")
    print("99th Percentile Job Completion Time: ", np.percentile(sorted_completion_times , 99), "seconds")
    print("Max Job Completion Time: ", np.max(sorted_completion_times))

    print("Average Scheduling Wait: ", np.average(np.array(scheduling_times)), "seconds")
    print("Average Executor Run Time: ", np.average(np.array(exec_run_times))/1000, "seconds")
    print("Average Executor Deserialize Time: ", np.average(np.array(exec_deserialize_times))/1000, "seconds")
    print("Disk Accesses: ", num_disk_access)
    print("Percent Disk Accesses: ", float(num_disk_access)/len(stagedata))
    print("Percent Cached: ", float(num_skipped)/len(stagedata))
    print("Disk Bytes Written: ", np.sum(np.array(disk_bytes_written)))
    print("Average Write Time: ", np.average(np.array(disk_write_times))/100, "seconds")
    print("Data Exchanges: ", num_exchanges)
    print("Bytes Exchanged: ", np.sum(np.array(num_exchanged_bytes)))
    #input()
    plt.close('all')

except requests.exceptions.RequestException as e:
    print(e)
