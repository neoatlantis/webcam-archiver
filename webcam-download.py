#!/usr/bin/env python3

import os
import sys
import yaml
import re
import time
import threading
import subprocess


def curl_command(url, output):
    tempfile = output + ".tmp"
    command = [
        "curl", url,
        "-H", "pragma: no-cache",
        "-H", "user-agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36",
        "--compressed",
        "--output", tempfile,
    ]
    try:
        subprocess.run(command)
        subprocess.run(["mv", tempfile, output])
    except Exception as e:
        print(e)
        subprocess.run(["rm", "-f", tempfile])


config = yaml.load(open(sys.argv[1], "r").read())

sources = config["sources"]
for source in sources:
    assert re.match("^[0-9a-z\\-]{1,64}$", source["id"])
    assert "url" in source
    assert "ext" in source
    period = source["period"] or 60
    assert type(period) == int

storage_path = config["storage-path"]

existing_files = [e for e in os.listdir(storage_path) if e.startswith("data:")]


existing_dataset = {}
for e in existing_files:
    filename, filename_ext = os.path.splitext(e)
    try:
        data, data_id, data_timestamp = filename.split(":")
        data_timestamp = int(data_timestamp)
    except:
        continue
    if (\
        data_id not in existing_dataset or
        data_timestamp > existing_dataset[data_id]\
    ):
        existing_dataset[data_id] = data_timestamp

nowtime = int(time.time())
threads = {}


for source in sources:
    source_id, source_period, source_url =\
        source["id"], source["period"], source["url"]
    source_ext = source["ext"]

    if (\
        source_id in existing_dataset and\
        nowtime - existing_dataset[source_id] < source_period\
    ):
        continue

    print("\t".join([source_id, source_url]))

    output_path = os.path.join(storage_path, "data:%s:%d.%s" % (
        source_id,
        nowtime,
        source_ext
    ))

    t = threading.Thread(target=curl_command, args=(source_url, output_path))
    threads[source_id] = t
    t.start()




keys = list(threads.keys())
keys.sort()
done = False
while True:
    done = True
    #os.system("clear")
    for t_id in keys:
        if t_id not in threads: continue
        if threads[t_id].is_alive():
            done = False
            print(t_id)
        else:
            del threads[t_id]
    if done: break
    time.sleep(0.5)
