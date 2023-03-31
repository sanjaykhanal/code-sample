import os
import sys
import json
import logging
import psutil
sys.path.append(os.path.abspath('..'))

from time import time, sleep
from multiprocessing import Process

from rtvtr.pipeline.frames_queue import FramesQueue
from rtvtr.transport import results_server
from rtvtr.transport.results_server import ResultsSocketServer
from rtvtr.pipeline.pipeline import PipeLine, prepare_nodes
from rtvtr.api.app.modules.engine.crud.camera import camera_list
from rtvtr.api.app.database.db import get_db

from rtvtr.utils.cpu_utils import get_cpu_sets_per_channel
from rtvtr.utils.utils import NpEncoder
from rtvtr.events.config import EVENT_LIST
from rtvtr.transport.redis_manager import RedisPubSubManager


TEST_SRC = '/home/laanta/Documents/RTVTR/RTVTR/rtvtr/tests/test3.MP4'
# TEST_SRC = None


try:
    with open('.channels') as f:
        data = f.readline()
    number_of_channels = int(float(data))
except Exception as e:
    print("error loading number of channels information. Falling back to using 4 channels")
    number_of_channels = 4

cpu_sets_per_channel = get_cpu_sets_per_channel(number_of_channels=number_of_channels)


OBJECT_DETECTION_EVENT_CHANNEL = "OBJECT_DETECTION"
OBJECT_DETECTED_EVENT = EVENT_LIST[OBJECT_DETECTION_EVENT_CHANNEL]["OBJECT_DETECTED"]
OBJECT_NOT_DETECTED_EVENT = EVENT_LIST[OBJECT_DETECTION_EVENT_CHANNEL]["OBJECT_NOT_DETECTED"]

last_object_events = {}
pubsub = RedisPubSubManager()


def object_detection_event(cam_id, object_type, object_count, event_name, data):

    last_message = {"camera_id": cam_id, "event_name": event_name, "count": object_count}

    if object_type not in last_object_events.keys():
        last_object_events[object_type] = last_message

    try:
        last_event = last_object_events[object_type]
    except KeyError as e:
        pass
    else:
        if last_event == last_message:
            return

    try:
        pubsub.publish(OBJECT_DETECTION_EVENT_CHANNEL, str({"camera_id": cam_id, "event_name": event_name, "count": object_count, "data": data}))
        last_object_events[object_type] = last_message
    except Exception as e:
        logging.error("error while publishing event '{}'".format(str(last_message)))
        logging.error(e)
        print(e)


def start_pipeline(cam_id, cpu_set):
    proc = psutil.Process()
    proc.cpu_affinity(cpu_set)
    result_socket = ResultsSocketServer(cam_id)
    frames_queue = FramesQueue(cam_id)
    frames_queue.start()
    pipeline = PipeLine(prepare_nodes(), output_socket=result_socket, cam_id=cam_id)

    while True:
        frame = frames_queue.get()
        if frame is not None:
            t1 = time()
            output, metadata = pipeline(frame)
            t2 = time()
            print("{}: time: {}".format(cam_id, t2-t1))
            if output:
                print(output['detections'])
                object_types = list(output['detections'].keys())

                for object_type in object_types:
                    count = len(output["detections"][object_type])
                    if count:
                        data = {"details": output["detections"][object_type], "object_type": object_type}
                        object_detection_event(cam_id, object_type, count, OBJECT_DETECTED_EVENT, data)
                    else:
                        data = {"count": 0, "object_type": object_type}
                        object_detection_event(cam_id, object_type, 0, OBJECT_NOT_DETECTED_EVENT, data)

pipeline_processes = {}


def spawn_process(cam_id, cpu_set):
    p = Process(name= 'camera_pipeline_{}'.format(cam_id),target=start_pipeline, args=(str(cam_id),cpu_set,), daemon=True)
    p.start()
    pipeline_processes[cam_id] = {"process": p, "cpu_set": cpu_set}

cpu_sets_available = [v for k,v in cpu_sets_per_channel.items()]


def spwan_process_for_all_cameras():

    cameras = camera_list(next(get_db()))
    if cameras:
        cameras = [cam.__dict__ for cam in cameras] #Only one camera is being used for testing
    print(cameras)
    previous_camera_ids = list(pipeline_processes.keys())
    new_camera_ids = [c["id"] for c in cameras]

    print("\n\n\n", previous_camera_ids)
    print(new_camera_ids, "\n\n\n")

    # Cleanup processes for removed cameras
    for camera_id in previous_camera_ids:
        if camera_id not in new_camera_ids:
            try:
                pipeline_processes[camera_id]["process"].kill()
                pipeline_processes[camera_id]["process"].close()
                cpu_sets_available.append(pipeline_processes[camera_id]["cpu_set"])
                del pipeline_processes[camera_id]
            except Exception as e:
                logging.error("Error while removing camera '{}' during hot reload".format(camera_id))
                logging.error(e)

    for camera in cameras:
        camera_id = camera["id"]

        if camera_id not in previous_camera_ids:
            cpu_set = cpu_sets_available.pop()
            spawn_process(camera_id, cpu_set)


while True:

    spwan_process_for_all_cameras()
    for k,v in pipeline_processes.items():
        is_alive = v["process"].is_alive()
        print('{}: {}'.format(k, 'alive' if is_alive else 'dead'))
        if not is_alive:
            v["process"].kill()
            v["process"].close()
            spawn_process(k, v["cpu_set"])

    sleep(5)
