
import logging
import smtplib
import threading
import os
import time

from datetime import date, datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from common.constants import ONE_DAY_IN_SECS
from cluster_manager.global_cluster_cache import GlobalClusterCache
from custom_exceptions.exceptions import SameTimestampError

global_cache = GlobalClusterCache()

cm_logger = logging.getLogger(__name__)
cm_logger.setLevel(logging.DEBUG)
handler = logging.FileHandler("cluster_monitor.log", mode='w')
formatter = logging.Formatter("%(filename)s:%(lineno)d - %(asctime)s %(levelname)s - %(message)s")
handler.setFormatter(formatter)
cm_logger.addHandler(handler)


class ClusterMonitor:
    """Class that actually monitors the cache (subsequently, the entire cluster setup)
        This is a singleton class.
    """
    # Singleton
    _g_cluster_monitor_instance = None
    _g_c_monitor_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._g_cluster_monitor_instance is None:
            with cls._g_c_monitor_lock:
                if cls._g_cluster_monitor_instance is None:
                    cls._g_cluster_monitor_instance = super().__new__(cls)
        return cls._g_cluster_monitor_instance

    def __init__(self):
        self.mailer = smtplib.SMTP()
        pass

    def send_warning_emails(self):
        pass

    def _calculate_continued_offenses(self, from_timestamp=None,
                                     new_timestamp=None,
                                     timediff=ONE_DAY_IN_SECS):
        continued_offenses = {}
        if new_timestamp is None:
            new_timestamp = int(time.time())
        if from_timestamp is None:
            from_timestamp = new_timestamp - timediff
        cm_logger.info(f'Getting the offenses done between {from_timestamp}:'
                       f'{date.fromtimestamp(from_timestamp)} to {new_timestamp}:'
                       f'{date.fromtimestamp(new_timestamp)}')

        try:
            timed_offenses = global_cache.get_timed_offenses(
                start_ts=from_timestamp,
                end_ts=new_timestamp
            )
        except SameTimestampError as ste:
            cm_logger.warning(ste)
            return
        actual_old_ts, old_offenses, actual_new_ts, new_offenses = timed_offenses
        cm_logger.info(f'Actual timestamps for the offenses is {actual_old_ts}:'
                       f'{date.fromtimestamp(actual_old_ts)} to {actual_new_ts}:'
                       f'{date.fromtimestamp(actual_new_ts)}')

        # Create a generic process that calculates the difference
        for k, old_v in old_offenses.items():
            if k in new_offenses:
                new_v = new_offenses[k]
                common_offender = None
                # If the values are dict, we need the keys
                if type(old_v) is dict:
                    common_offenders = set(old_v.keys()).intersection(set(new_v.keys()))
                    # For the dict, insert the key into the tracker, and insert the latest values
                    continued_offenses[k] = {}
                    for co in common_offenders:
                        continued_offenses[k][co] = new_v[co]
                # if the values is a list
                elif type(old_v) is list:
                    # list of tuples. NOTE: First value (idx 0) is assumed to
                    # be the UUID (Names are also fine if they are immutable)
                    if old_v[0]:
                        if old_v[0] is tuple:
                            common_offenders = set([v[0] for v in old_v]).intersection(set([v[0] for v in new_v]))
                            # Reconstruct the tuple
                            continued_offenses[k] = [v for v in new_v if v[0] in common_offenders]
                        else:
                            # List of values (typically strings) TODO Handle other cases
                            common_offenders = set(old_v).intersection(set(new_v))
                            continued_offenses[k] = common_offenders
        pass

    def take_action_offenses(self):
        current_time = time.time()
        first_action_run = float(os.environ.get("first_action_run"))
        if current_time < first_action_run:
            cm_logger.info(f"Triggered action at {datetime.fromtimestamp(current_time)}. First should not start before {datetime.fromtimestamp(first_action_run)}")
            return
        start_time = time.time() - ONE_DAY_IN_SECS
        self._calculate_continued_offenses(from_timestamp=start_time, new_timestamp=current_time)
