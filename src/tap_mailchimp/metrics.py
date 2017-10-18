"""Supplement to singer.metrics: Utilities for logging metrics."""

import time
from singer import Counter
from singer.metrics import log, Point, Tag, DEFAULT_LOG_INTERVAL

class ProgressCounter(Counter):
    def __init__(self, total_items=None, tags=None, metric='progress_counter',
                 log_interval=DEFAULT_LOG_INTERVAL):
        tags = tags or {}
        self.total_items = total_items
        if self.total_items:
            tags['total_items'] = self.total_items
        super().__init__(metric, tags=tags, log_interval=log_interval)
    def _pop(self):
        log(self.logger, Point('progress', self.metric, self.value, self.tags))
        self.last_log_time = time.time()

def progress_counter(total_items=None, endpoint=None, tags=None,
                     log_interval=DEFAULT_LOG_INTERVAL):
    tags = dict(tags) if tags else {}
    if endpoint:
        tags[Tag.endpoint] = endpoint
    return ProgressCounter(total_items, tags=tags, log_interval=log_interval)
