import time
from datetime import datetime
import dateutil
from singer import write_state
from .jsonext import JsonObject

SYNC_STATE_INTERVAL = 60


class Keys:
    last_run = 'last_run'
    current_run = 'current_run'
    currently_syncing = 'currently_syncing'
    bookmarks = 'bookmarks'
    ids = 'ids'
    done = 'done'
    count = 'count'
    offset = 'offset'


class TapState(JsonObject):

    def __init__(self, state=None):
        state = state or {}
        self.last_run = state.get(Keys.last_run)
        self.current_run = state.get(Keys.current_run,
                                     datetime.now(dateutil.tz.tzutc()))
        self.currently_syncing = state.get(Keys.currently_syncing)
        self.bookmarks = state.get(Keys.bookmarks, {})
        self._current_session = time.time()
        self._last_sync_state = time.time()

    def __json__(self):
        return {Keys.last_run: self.last_run,
                Keys.current_run: self.current_run,
                Keys.currently_syncing: self.currently_syncing,
                Keys.bookmarks: self.bookmarks}

    def finalize_run(self):
        self.last_run = self.current_run
        self.current_run = None
        self.currently_syncing = None
        self.bookmarks = {}

    def session_time(self):
        return time.time() - self._current_session

    @property
    def last_run(self):
        return self._last_run

    @last_run.setter
    def last_run(self, value):
        if value is None or isinstance(value, datetime):
            self._last_run = value
        else:
            self._last_run = dateutil.parser.parse(value)

    @property
    def current_run(self):
        return self._current_run

    @current_run.setter
    def current_run(self, value):
        if value is None or isinstance(value, datetime):
            self._current_run = value
        else:
            self._current_run = dateutil.parser.parse(value)

    def sync(self, force=False):
        if force or (time.time() - self._last_sync_state > SYNC_STATE_INTERVAL):
            self.write_state()

    def write_state(self):
        now = time.time()
        state = self.__json__()
        for k, v in state.items():
            if isinstance(v, datetime):
                state[k] = v.isoformat()
        write_state(state)
        self._last_sync_state = now

    def get_bookmark(self, stream_id, key, default=None):
        return self.bookmarks.get(stream_id, {}).get(key, default)

    def write_bookmark(self, stream_id, key, val):
        self.bookmarks.setdefault(stream_id, {})[key] = val

    def get_offset(self, stream_id, default=None):
        return self.get_bookmark(stream_id, Keys.offset, default)

    def set_offset(self, stream_id, offset_key, offset_value):
        offset = self.get_offset(stream_id, {})
        offset[offset_key] = offset_value
        self.write_bookmark(stream_id, Keys.offset, offset)

    def clear_offset(self, stream_id):
        self.write_bookmark(stream_id, Keys.offset, {})

    def get_ids(self, stream_id):
        return set(self.get_bookmark(stream_id, Keys.ids, []))

    def add_id(self, stream_id, id_):
        ids = set(self.get_ids(stream_id))
        ids.add(id_)
        self.write_bookmark(stream_id, Keys.ids, list(ids))

    def get_id_offset(self, stream_id, id_, default=None):
        try:
            return self.get_offset(stream_id)[id_]
        except (TypeError, KeyError):
            return default

    def set_id_offset(self, stream_id, id_, offset_key, offset_value):
        offset = self.get_id_offset(stream_id, id_, {})
        offset[offset_key] = offset_value
        self.set_offset(stream_id, id_, offset)

    def get_done(self, stream_id):
        return self.get_bookmark(stream_id, Keys.done, False)

    def set_done(self, stream_id, done=True):
        self.write_bookmark(stream_id, Keys.done, done)
        if done:
            self.clear_offset(stream_id)

    def get_id_done(self, stream_id, id_):
        return self.get_id_offset(stream_id, id_, {}).get(Keys.done, False)

    def set_id_done(self, stream_id, id_, done=True):
        if done:
            # clear all other offset state for this id
            self.set_offset(stream_id, id_, {Keys.done: done})
        else:
            self.set_id_offset(stream_id, id_, Keys.done, done)

    def get_count(self, stream_id):
        return self.get_offset(stream_id, {}).get(Keys.count, 0)

    def set_count(self, stream_id, count):
        self.write_bookmark(stream_id, Keys.count, count)

    def get_id_count(self, stream_id, id_):
        return self.get_id_offset(stream_id, id_, {}).get(Keys.count, 0)

    def set_id_count(self, stream_id, id_, count):
        self.set_id_offset(stream_id, id_, Keys.count, count)
