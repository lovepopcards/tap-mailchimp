"""MailChimpTap for singer.io."""

import datetime as dt
import dateutil as du
import mailchimp3
import singer
import tap_mailchimp.config as cfg
import tap_mailchimp.utils as taputils

def mailchimp_gen(base, attr, item_key=None, **kwargs):
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
    :param item_key: Key for list items (optional, defaults to attr)
    :param **kwargs: Arguments passed on to the API object's `all` method (e.g.
                     list_id for list members)
    """
    process_count, total_items = 0, 1
    if item_key is None:
        item_key = attr
    api_method = getattr(base, attr)
    while process_count < total_items:
        with singer.http_request_timer(endpoint=attr):
            response = api_method.all(offset=process_count, **kwargs)
        items, total_items = taputils.pluck(response, item_key, 'total_items')
        for i in items:
            yield i
            process_count += 1
        # TODO find more elegant way to test early stopping!
        return


class MailChimpTap:
    """Singer.io tap for MailChimp API v3.

    Usage:
        >>> tap = MailChimpTap(config, state)
        >>> tap.pour()

    :param dict config: Configuration object from singer.
    :param dict state: State object from singer.
    """

    def __init__(self, config, state, catalog=None):
        self.config = config
        self.state = state
        username, api_key = config[cfg.username], config[cfg.api_key]
        self.client = mailchimp3.MailChimp(username, api_key)
        self.catalog = catalog

    @property
    def catalog(self):
        """singer.Catalog for the MailChimp v3 API."""
        if self._catalog is None:
            self._catalog = self.discover()
        return self._catalog

    @catalog.setter
    def catalog(self, c):
        self._catalog = c

    def discover(self):
        """Return a singer.io Catalog for the MailChimp v3 API.

        :return: Singer.io Catalog instance.
        :rtype: singer.Catalog
        """
        catalog = {'streams': [{'stream': s,
                                'tap_stream_id': s,
                                'key_properties': cfg.key_properties[s],
                                'schema': taputils.get_schema(s)}
                               for s in cfg.all_streams]}
        return singer.Catalog.from_dict(catalog)

    def start_date(self, stream, use_lag=True):
        """Return the time to start the stream.

        Subtract lag_days given in configuration. This makes the assumption
        that for most campaign email activity we care about activity within
        lag_days.

        :param str stream: Stream name for which to get the date.
        :param boolean use_lag: Whether to use lag days from config, optional,
                                default is True.
        :return: Time to start the stream, or None to get data for all time.
        :rtype: datetime.datetime
        """
        mark = singer.get_bookmark(self.state, stream, cfg.last_record)
        if mark:
            start = du.parser.parse(mark)
        else:
            cfg_start = self.config[cfg.start_date]
            if cfg_start == '' or cfg_start == '*':
                return None
            start = du.parser.parse(cfg_start)
        if use_lag:
            lag_days = dt.timedelta(days=self.config[cfg.lag_days])
            return start - lag_days
        else:
            return start

    def pour(self):
        """Pour schemata and data from the Mailchimp tap."""
        with singer.job_timer(job_type='mailchimp'):
            for s in self.catalog.streams:
                pour_method = getattr(self, 'pour_{}'.format(s.stream))
                pour_method()
                singer.write_state(self.state)
            singer.write_state(self.state)

    def _gen_args(self, stream, date_key=None, with_count=True, use_lag=True):
        args = {}
        if with_count:
            args['count'] = self.config[cfg.count]
        if date_key is not None:
            start_date = self.start_date(stream, use_lag=use_lag)
            if start_date:
                args[date_key] = start_date.isoformat()
        return args

    def pour_lists(self):
        name = cfg.lists
        with singer.job_timer(job_type=name):
            stream = self.catalog.get_stream(name)
            singer.set_currently_syncing(self.state, stream.tap_stream_id)
            singer.write_schema(name, stream.schema.to_dict(), stream.key_properties, stream.stream_alias)
            args = self._gen_args(name, date_key=cfg.date_key[name])
            dates = []
            with singer.record_counter(endpoint=cfg.endpoint[name]) as counter:
                for record in mailchimp_gen(self.client, cfg.api_attr[name], **args):
                    singer.write_record(name, record)
                    try:
                        dates.append(du.parser.parse(record['stats']['campaign_last_sent']))
                    except:
                        pass
                    counter.increment()
            if len(dates) > 0:
                max_date = max(dates)
                singer.write_bookmark(self.state, name, cfg.last_record, max_date.isoformat())

    def _list_ids(self, stream):
        name = cfg.lists
        args = self._gen_args(stream, date_key=cfg.date_key[name], with_count=False)
        with singer.http_request_timer(endpoint=name):
            response = self.client.lists.all(get_all=True, fields='lists.id', **args)
        return set([item['id'] for item in response['lists']])

    def pour_list_members(self):
        name = cfg.list_members
        with singer.job_timer(job_type=name):
            stream = self.catalog.get_stream(name)
            singer.set_currently_syncing(self.state, stream.tap_stream_id)
            singer.write_schema(name, stream.schema.to_dict(), stream.key_properties, stream.stream_alias)
            args = self._gen_args(name, cfg.date_key[name], use_lag=False)
            list_ids = self._list_ids(name)
            dates = []
            with singer.record_counter(endpoint=cfg.endpoint[name]) as counter:
                for id in list_ids:
                    for record in mailchimp_gen(self.client.lists, cfg.api_attr[name], list_id=id, **args):
                        singer.write_record(name, record)
                        try:
                            dates.append(du.parser.parse(record['last_changed']))
                        except (KeyError, ValueError):
                            pass
                        counter.increment()
            if len(dates) > 0:
                max_date = max(dates)
                singer.write_bookmark(self.state, name, cfg.last_record, max_date.isoformat())

    def pour_campaigns(self):
        name = cfg.campaigns
        with job_timer(job_type=name):
            stream = self.catalog.get_stream(name)
            singer.set_currently_syncing(self.state, stream.tap_stream_id)
            singer.write_schema(name, stream.schema.to_dict(), stream.key_properties, stream.stream_alias)
            args = self._gen_args(name, cfg.date_key[name])
            dates = []
            with singer.record_counter(endpoint=cfg.endpoint[name]) as counter:
                for record in mailchimp_gen(self.client, cfg.api_attr[name], **args):
                    singer.write_record(name, record)
                    try:
                        dates.append(du.parser.parse(record['send_time']))
                    except (KeyError, ValueError):
                        pass
                    counter.increment()
            if len(dates) > 0:
                max_date = max(dates)
                singer.write_bookmark(self.state, name, cfg.last_record, max_date.isoformat())

    def _campaign_ids(self, stream):
        name = cfg.campaigns
        args = self._gen_args(stream, date_key=cfg.date_key[name], with_count=False)
        with singer.http_request_timer(endpoint=name):
            response = self.client.campaigns.all(get_all=True, fields='campaigns.id', **args)
        return set([item['id'] for item in response['campaigns']])

    def pour_email_activity_reports(self):
        name = cfg.email_activity_reports
        with singer.job_timer(job_type=name):
            stream = self.catalog.get_stream(name)
            singer.set_currently_syncing(self.state, stream.tap_stream_id)
            singer.write_schema(name, stream.schema.to_dict(), stream.key_properties, stream.stream_alias)
            args = self._gen_args(name)
            campaign_ids = self._campaign_ids(name)
            with singer.record_counter(endpoint=endpoint) as counter:
                for id in campaign_ids:
                    for record in mailchimp_gen(self.client.reports, 'email_activity',
                                                  campaign_id=id, **args):
                        singer.write_record(name, record)
                        counter.increment()
