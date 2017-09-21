"""MailChimpTap for singer.io."""

import time
import itertools
import json
import datetime as dt
import dateutil as du
from mailchimp3 import MailChimp
from singer import ( write_state, write_record, write_schema,
                     record_counter, job_timer, http_request_timer )
from singer.logger import get_logger
from singer.bookmarks import ( write_bookmark, get_bookmark, set_offset,
                               clear_offset, get_offset, set_currently_syncing,
                               get_currently_syncing )
from tap_mailchimp.utils import get_schema

DEFAULT_COUNT = 50
DEFAULT_LAG = 3
MAX_RETRIES = 5
SYNC_STATE_INTERVAL = 60


def log_progress(logger, count, total, endpoint, **extra_tags):
    data = {
        'type': 'progress',
        'metric': 'progress_counter',
        'value': count,
        'tags': { 'endpoint': endpoint, 'total_items': total, **extra_tags }
    }
    logger.info('METRIC: %s', json.dumps(data))


def mailchimp_gen(base, attr, endpoint=None, item_key=None, offset=0,
                  count=DEFAULT_COUNT, **kwargs):
    """Generator to iterate over mailchimp responses.

    Mailchimp returns a subset of total data, and uses an `offset` parameter to
    iterate. We fetch and cache the next chunk of data, then call the API as
    needed.

    Usage:

        >>> for item in mailchimp_gen(client, 'lists'):
        >>>     process(item)
        >>> for item in mailchimp_gen(client, 'authorized_apps', item_key='apps'):
        >>>     process(item)

    Args:

    :param base: mailchimp3.MailChimp or API child object

    :param attr: Name of child object to base the generator on

    :param endpoint: Endpoint name for singer.http_request_timer (optional,
                     defaults to attr)

    :param item_key: Key for list items (optional, defaults to attr)

    :param offset: Offset from which to start (optional, defaults to 0)

    :param **kwargs: Arguments passed on to the API object's `all` method (e.g.
                     list_id for list members)
    """
    process_count = offset
    total_items = None
    if item_key is None:
        item_key = attr
    if endpoint is None:
        endpoint = attr
    api_method = getattr(base, attr)
    logger = get_logger()
    while total_items is None or process_count < total_items:
        retry_count = 0
        while retry_count < MAX_RETRIES:
            retry_count += 1
            try:
                with http_request_timer(endpoint=endpoint):
                    response = api_method.all(offset=process_count, count=count, **kwargs)
                break
            except Exception as e:
                logger.error('Attempt {}/{}: {}'.format(retry_count, MAX_RETRIES, e))
                if retry_count == MAX_RETRIES:
                    raise
                time.sleep(2 ** retry_count)
        items = response[item_key]
        if total_items is None:
            total_items = response['total_items']
        for i in items:
            yield i
            process_count += 1
        log_progress(logger, process_count, total_items, endpoint, **kwargs)


class MailChimpTap:
    """Singer.io tap for MailChimp API v3.

    Usage:
        >>> tap = MailChimpTap(config, state)
        >>> tap.pour()

    :param config: Configuration object from singer.
    :param state: State object from singer.
    :param client: MailChimp client (optional, created if None supplied)
    :param logger: Logging object (optional, singer logger if None supplied)
    """

    def __init__(self, config, state, client=None, logger=None):
        self.config = config
        self.state = state
        username, api_key = config['username'], config['api_key']
        self.client = client if client else MailChimp(username, api_key)
        self.logger = logger if logger else get_logger()
        self.last_sync_state = time.time()

    @property
    def count(self):
        return self.config.get('count', DEFAULT_COUNT)

    @property
    def start_date(self):
        try:
            return du.parser.parse(self.state['last_record'])
        except (TypeError, KeyError, ValueError):
            date = self.config['start_date']
            if date == '' or date == '*':
                return None
            return du.parser.parse(date)

    @property
    def lag(self):
        return self.config.get('lag_days', DEFAULT_LAG)

    @property
    def lag_date(self):
        if self.start_date is None:
            return None
        return self.start_date - dt.timedelta(days=self.lag)

    def sync_state(self):
        now = time.time()
        if now - self.last_sync_state > SYNC_STATE_INTERVAL:
            write_state(self.state)
            self.last_sync_state = now

    def pour(self):
        """Pour schemata and data from the Mailchimp tap."""
        if not self.state.get('current_run'):
            self.state['current_run'] = dt.datetime.now(du.tz.tzutc()).isoformat()
        with job_timer(job_type='mailchimp'):
            for stream_id in ('lists', 'list_members', 'campaigns',
                           'email_activity_reports'):
                self.pour_stream(stream_id)
        self.state = {'last_record': self.state['current_run']}
        write_state(self.state)

    def pour_stream(self, stream_id):
        if self.is_done(stream_id):
            self.log_skip(stream_id, 'done')
            return
        self.log_start(stream_id, offset=self.get_offset_count(stream_id))
        set_currently_syncing(self.state, stream_id)
        stream_fn = getattr(self, 'pour_' + stream_id)
        with job_timer(job_type=stream_id), record_counter(endpoint=stream_id) as counter:
            stream_fn(counter)
        write_bookmark(self.state, stream_id, 'done', True)
        clear_offset(self.state, stream_id)
        self.sync_state()
        self.log_finish(stream_id)

    def log_info(self, stream_id, **tags):
        self.logger.info(json.dumps({'stream': stream_id, **tags}))

    def log_skip(self, stream_id, reason, **extra_tags):
        self.log_info(stream_id, action='skip', reason=reason, **extra_tags)

    def log_start(self, stream_id, **extra_tags):
        self.log_info(stream_id, action='start', **extra_tags)

    def log_finish(self, stream_id, **extra_tags):
        self.log_info(stream_id, action='finish', **extra_tags)

    def get_offset_count(self, stream_id, item_id=None):
        try:
            if item_id is None:
                return get_offset(self.state, stream_id)['count']
            else:
                return get_offset(self.state, stream_id)[item_id]['count']
        except (TypeError, KeyError):
            return 0

    def is_done(self, stream_id, item_id=None):
        try:
            if item_id is None:
                return get_bookmark(self.state, stream_id, 'done')
            else:
                return get_offset(self.state, stream_id)[item_id]['done']
        except (TypeError, KeyError):
            return False

    def get_ids(self, stream_id):
        try:
            return set(get_bookmark(self.state, stream_id, 'ids'))
        except (TypeError, KeyError):
            return set()

    def pour_lists(self, counter):
        name = 'lists'
        list_ids = self.get_ids(name)
        schema = get_schema(name)
        write_schema(name, schema, key_properties='id')
        offset_count = self.get_offset_count(name)
        for record in mailchimp_gen(self.client,
                                    name,
                                    count=self.count,
                                    offset=offset_count):
            write_record(name, record)
            list_ids.add(record['id'])
            write_bookmark(self.state, name, 'ids', list(list_ids))
            set_offset(self.state, name, 'count', len(list_ids))
            self.sync_state()
            counter.increment()

    def pour_list_members(self, offset, counter):
        name = 'list_members'
        schema = get_schema(name)
        write_schema(name, schema, key_properties='id')
        args = {}
        if self.start_date is not None:
            args['since_last_changed'] = self.start_date.isoformat()
        list_ids = self.get_ids('lists')
        for id in list_ids:
            if self.is_done(name, id):
                self.log_skip(name, 'done', list_id=id)
                continue
            offset_count = current_counter = self.get_offset_count(name, id)
            self.log_start(name, list_id=id, offset=offset_count)
            for record in mailchimp_gen(self.client.lists,
                                        'members',
                                        endpoint=name,
                                        list_id=id,
                                        count=self.count,
                                        offset=offset_count,
                                        **args):
                write_record(name, record)
                current_counter += 1
                set_offset(self.state, name, id, {'count': current_counter})
                self.sync_state()
                counter.increment()
            set_offset(self.state, name, id, {'done': True})
            self.sync_state()
            self.log_finish(name, list_id=id)

    def pour_campaigns(self, counter):
        name = 'campaigns'
        campaign_ids = self.get_ids(name)
        schema = get_schema(name)
        write_schema(name, schema, key_properties='id')
        offset_count = self.get_offset_count(name)
        if self.lag_date is None:
            gen = mailchimp_gen(self.client, name, count=self.count, offset=offset_count)
        else:
            gen_create = mailchimp_gen(self.client,
                                       name,
                                       count=self.count,
                                       since_create_time=self.start_date.isoformat(),
                                       offset=offset_count)
            gen_send = mailchimp_gen(self.client,
                                     name,
                                     count=self.count,
                                     since_send_time=self.lag_date.isoformat(),
                                     offset=offset_count)
            gen = itertools.chain(gen_create, gen_send)
        for record in gen:
            write_record(name, record)
            campaign_ids.add(record['id'])
            write_bookmark(self.state, name, 'ids', list(campaign_ids))
            set_offset(self.state, name, 'count', len(campaign_ids))
            self.sync_state()
            counter.increment()

    def pour_email_activity_reports(self, counter):
        name = 'email_activity_reports'
        schema = get_schema(name)
        write_schema(name, schema, key_properties=['campaign_id', 'email_id'])
        campaign_ids = self.get_ids('campaigns')
        for id in campaign_ids:
            if self.is_done(name, id):
                self.log_skip(name, 'done', list_id=id)
                continue
            offset_count = current_counter = self.get_offset_count(name, id)
            self.log_start(name, campaign_id=id, offset=offset_count)
            for record in mailchimp_gen(self.client.reports, 'email_activity', endpoint=name,
                                        item_key='emails', campaign_id=id, count=self.count):
                write_record(name, record)
                current_counter += 1
                set_offset(self.state, name, id, {'count': current_counter})
                self.sync_state()
                counter.increment()
            set_offset(self.state, name, id, {'done': True})
            self.sync_state()
            self.log_finish(name, campaign_id=id)
