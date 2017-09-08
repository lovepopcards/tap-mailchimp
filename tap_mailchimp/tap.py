"""MailChimpTap for singer.io."""

import time
import itertools
import json
import datetime as dt
import dateutil as du
import mailchimp3
import singer
import singer.logger
import tap_mailchimp.utils as taputils

DEFAULT_COUNT = 50
DEFAULT_LAG = 7
MAX_RETRIES = 5


def log_progress(logger, count, total, endpoint):
    data = {
        'type': 'progress',
        'metric': 'progress_counter',
        'value': count,
        'tags': { 'endpoint': endpoint, 'total_items': total }
    }
    logger.info('METRIC: %s', json.dumps(data))


def mailchimp_gen(base, attr, endpoint=None, item_key=None, **kwargs):
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

    :param **kwargs: Arguments passed on to the API object's `all` method (e.g.
                     list_id for list members)
    """
    process_count, total_items = 0, 1
    if item_key is None:
        item_key = attr
    if endpoint is None:
        endpoint = attr
    api_method = getattr(base, attr)
    logger = singer.logger.get_logger()
    while process_count < total_items:
        retry_count = 0
        while retry_count < MAX_RETRIES:
            retry_count += 1
            try:
                with singer.http_request_timer(endpoint=endpoint):
                    response = api_method.all(offset=process_count, **kwargs)
                break
            except Exception as e:
                logger.error('Attempt {}/{}: {}'.format(retry_count, MAX_RETRIES, e))
                if retry_count == MAX_RETRIES:
                    raise
                time.sleep(2 ** retry_count)
        items, total_items = taputils.pluck(response, item_key, 'total_items')
        for i in items:
            yield i
            process_count += 1
        log_progress(logger, process_count, total_items, endpoint)


class MailChimpTap:
    """Singer.io tap for MailChimp API v3.

    Usage:
        >>> tap = MailChimpTap(config, state)
        >>> tap.pour()

    :param dict config: Configuration object from singer.
    :param dict state: State object from singer.
    """

    def __init__(self, config, state):
        self.config = config
        self.state = state
        username, api_key = config['username'], config['api_key']
        self.client = mailchimp3.MailChimp(username, api_key)
        self.last_record = state.get('last_record')

    @property
    def count(self):
        return self.config.get('count', DEFAULT_COUNT)

    @property
    def start_date(self):
        try:
            return du.parser.parse(self.state['last_record'])
        except KeyError:
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

    def pour(self):
        """Pour schemata and data from the Mailchimp tap."""
        with singer.job_timer(job_type='mailchimp'):
            self.last_record = dt.datetime.now(du.tz.tzutc())
            # list_ids = self.pour_lists()
            # self.pour_list_members(list_ids)
            campaign_ids = self.pour_campaigns()
            self.pour_email_activity_reports(campaign_ids)
            self.state.update({'last_record': self.last_record.isoformat()})
            singer.write_state(self.state)

    def pour_lists(self):
        name = 'lists'
        list_ids = set()
        with singer.job_timer(job_type=name):
            schema = taputils.get_schema(name)
            singer.write_schema(name, schema, key_properties='id')
            with singer.record_counter(endpoint=name) as counter:
                for record in mailchimp_gen(self.client, name, count=self.count):
                    singer.write_record(name, record)
                    list_ids.add(record['id'])
                    counter.increment()
        return list_ids

    def pour_list_members(self, list_ids):
        name = 'list_members'
        with singer.job_timer(job_type=name):
            schema = taputils.get_schema(name)
            singer.write_schema(name, schema, key_properties='id')
            args = {}
            if self.start_date is not None:
                args['since_last_changed'] = self.start_date.isoformat()
            with singer.record_counter(endpoint=name) as counter:
                for id in list_ids:
                    for record in mailchimp_gen(self.client.lists, 'members', endpoint=name,
                                                list_id=id, count=self.count, **args):
                        singer.write_record(name, record)
                        counter.increment()

    def pour_campaigns(self):
        name = 'campaigns'
        campaign_ids = set()
        with singer.job_timer(job_type=name):
            schema = taputils.get_schema(name)
            singer.write_schema(name, schema, key_properties='id')
            if self.lag_date is None:
                gen = mailchimp_gen(self.client, name, count=self.count)
            else:
                gen_create = mailchimp_gen(self.client, name, count=self.count,
                                           since_create_time=self.start_date.isoformat())
                gen_send = mailchimp_gen(self.client, name, count=self.count,
                                         since_send_time=self.lag_date.isoformat())
                gen = itertools.chain(gen_create, gen_send)
            with singer.record_counter(endpoint=name) as counter:
                for record in gen:
                    if record['id'] in campaign_ids:
                        # already processed (duplicate); happens when created
                        # and send dates after cutoff
                        continue
                    singer.write_record(name, record)
                    campaign_ids.add(record['id'])
                    counter.increment()
        return campaign_ids

    def pour_email_activity_reports(self, campaign_ids):
        name = 'email_activity_reports'
        with singer.job_timer(job_type=name):
            schema = taputils.get_schema(name)
            singer.write_schema(name, schema, key_properties=['campaign_id', 'email_id'])
            with singer.record_counter(endpoint=name) as counter:
                for id in campaign_ids:
                    for record in mailchimp_gen(self.client.reports, 'email_activity', endpoint=name,
                                                item_key='emails', campaign_id=id, count=self.count):
                        singer.write_record(name, record)
                        counter.increment()
