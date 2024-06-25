"""
Defines the 

Copyright (c) 2024 Nutanix Inc. All rights reserved.

Author:
    Sahil Naphade (sahil.naphade@nutanix.com)
"""

import logging
import schedule
import threading
import time

task_logger = logging.getLogger(__name__)
handler = logging.FileHandler(f"cmgr_tasks.log", mode='w')
formatter = logging.Formatter("%(name)s - %(asctime)s %(levelname)s - %(message)s")
handler.setFormatter(formatter)
task_logger.addHandler(handler)


class TaskManager:
    task_mgr_instance = None
    task_mgr_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls.task_mgr_instance is None:
            with cls.task_mgr_lock:
                if cls.task_mgr_instance is None:
                    cls.task_mgr_instance = super().__new__(cls)
        return cls.task_mgr_instance

    def __init__(self):
        # A schedule object to run repetitive tasks
        self.schedule = schedule

        # A thread to pick and run the scheduled tasks
        self.continuous_task_thread = threading.Thread(target=self._run_continuously)
        self.cont_t_lock = threading.Lock()
        self.continuous_tasks = set()

        # Another thread to run one-off tasks
        # self.single_r_t_lock = threading.Lock()
        # self.single_run_tasks = []
        # self.single_run_thread = threading.Thread(target=self._run_single_tasks)
        # self.single_task_tpool = futures.ThreadPoolExecutor(max_workers=3)

    def start_task_runner(self):
        task_logger.info("Starting the threads for taskmgr")
        self.continuous_task_thread.start()
        task_logger.info("Started the threads for taskmgr")

    # Used by auto-schedule
    def add_repeated_task(self, class_obj, method_name, job_schedule_args,
                          task_name=None, *args, **kwargs):
        """Adds a task which repeats every time-interval
        Args:
            class_obj: The object for which we have to run the function
            method_name: Class method of the class_obj
            job_schedule_args (dict): Dict which specifies the interval of the task
            task_name (str | Optional): Human-readable name of the task
            args: Args to be passed to this task
            kwargs: Key-word args to be passed to this task
        """
        tname = method_name.__name__ if task_name is None else task_name
        print(f"RECEIVED {tname} for running continuously")

        # Periodic tasks -- run "every" X time-interval
        seconds = job_schedule_args.get("seconds", None)
        minutes = job_schedule_args.get("minutes", None)
        hour = job_schedule_args.get("hour", None)
        day_time = job_schedule_args.get("day_time", None)

        populated_fields = sum([1 for value in [seconds, minutes, hour, day_time]
                                if value is not None])
        if populated_fields != 1:
            raise Exception(f"Expected exactly one field to be populated"
                            f" out of minutes, hour and day. Received "
                            f"{populated_fields}")

        class_fn = getattr(class_obj, method_name)
        if seconds:
            self.schedule.every(seconds).seconds.do(class_fn, *args, **kwargs)
        elif minutes:
            self.schedule.every(minutes).minutes.do(class_fn, *args, **kwargs)
        elif hour:
            self.schedule.every(hour).hour.do(class_fn, *args, **kwargs)
        elif day_time:
            self.schedule.every().day.at(day_time).do(class_fn, *args, **kwargs)

        task_info = f"{minutes} minutes" if minutes else f"{hour} hour"\
            if hour else f"{seconds} seconds" if seconds else "day at time: {}".format(day_time)
        print(f"Added repeated task '{tname}' to run every "
              f"{task_info}")

    def _run_continuously(self):
        """Used by the class thread for continuous tasks.
            Actually schedules the tasks.

            self.schedule stores the info, and then calls _run_threaded to
            create a thread and run the scheduled task on it.
        """
        while True:
            self.schedule.run_pending()
            time.sleep(1)

