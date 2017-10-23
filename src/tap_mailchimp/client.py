"""Read client for MailChimp API v3 API.

We use this to wrap the mailchimp3_ client and augment with:

1. Schema support.
2. Streaming responses especially for large datasets.
3. Access to the `MailChimp export API`_.

.. _mailchimp3: https://pypi.python.org/pypi/mailchimp3

.. _`MailChimp export API`:
   https://developer.mailchimp.com/documentation/mailchimp/guides/how-to-use-the-export-api/
"""

from collections import namedtuple
from datetime import datetime
from json.decoder import JSONDecodeError
from contextlib import closing
from urllib.parse import urlparse
import requests
from mailchimp3 import MailChimp as MailChimp3ApiClient
from singer import Timer
from singer.metrics import Metric, Tag
from .utils import (walk, datify, datify_or_none, int_or_float,
                    mailchimp_email_id, set_deep)
import tap_mailchimp.logger as logger
import tap_mailchimp.jsonext as json
from .metrics import progress_counter


class SubscriberActivityExportError(Exception):
    pass


class Status:
    subscribed = 'subscribed'
    unsubscribed = 'unsubscribed'
    cleaned = 'cleaned'
    _available = (subscribed, unsubscribed, cleaned)


class HashAlgo:
    sha256 = 'sha256'
    _available = (sha256,)


class MailChimp:
    def __init__(self, user_name, api_key, user_agent=None, timeout=None,
                 request_headers=None, exclude_links=False, **kwargs):
        self.exclude_links = exclude_links
        self._user_name = user_name
        self._api_key = api_key
        self._timeout = timeout
        self._headers = request_headers or requests.utils.default_headers()
        if user_agent is not None:
            self._headers['User-Agent'] = user_agent
        self._mc3 = MailChimp3ApiClient(user_name, api_key, timeout=timeout,
                                        request_headers=self._headers, **kwargs)

    def list_export(self, list_id, status=Status.subscribed, segment=None,
                    since=None, hashed=None):
        post_data = {'apikey': self._api_key,
                     'id': list_id,
                     'status': status}
        if segment:
            post_data['segment'] = segment
        since = self._format_time_for_export(since)
        if since:
            post_data['since'] = since
        if hashed:
            post_data['hashed'] = hashed
        url = '{}/list/'.format(self._export_base)
        with Timer(Metric.http_request_duration,
                   {Tag.endpoint: 'list_export',
                    'url': url,
                    'list_id': list_id,
                    'status': status,
                    'segment': segment,
                    'since': since,
                    'hashed': hashed}):
            response = requests.post(url, data=post_data, stream=True,
                                     timeout=self._timeout,
                                     headers=self._headers)
        with closing(response):
            _iter = response.iter_lines()
            first_line = next(_iter)
            if isinstance(first_line, bytes):
                first_line = first_line.decode('utf-8')
            headers = json.loads(first_line)
            for l in _iter:
                if isinstance(l, bytes):
                    l = l.decode('utf-8')
                record = json.loads(l)
                yield dict(zip(headers, record))

    def list_export_api_v3(self, list_id, status=Status.subscribed, **kwargs):
        merge_fields_gen = self.iter_items('lists.merge_fields', list_id=list_id,
                                           get_all=True)
        mappings = ApiVersionTool.api_v3_map_with_merge_fields(merge_fields_gen)
        for item in self.list_export(list_id, status, **kwargs):
            yield ApiVersionTool.coerce_list_export_to_api_v3(mappings, list_id,
                                                              status, item)

    def subscriber_activity_export(self, campaign_id, include_empty=False,
                                   since=None):
        post_data = {'apikey': self._api_key,
                     'id': campaign_id,
                     'include_empty': include_empty}
        since = self._format_time_for_export(since)
        if since:
            post_data['since'] = since
        url = '{}/campaignSubscriberActivity/'.format(self._export_base)
        with Timer(Metric.http_request_duration,
                   {Tag.endpoint: 'subscriber_activity_export',
                    'url': url,
                    'campaign_id': campaign_id,
                    'include_empty': include_empty,
                    'since': since}):
            response = requests.post(url,
                                     data=post_data,
                                     stream=True,
                                     timeout=self._timeout,
                                     headers=self._headers)
        with closing(response):
            for l in response.iter_lines():
                if isinstance(l, bytes):
                    l = l.decode('utf-8')
                # ignore empty lines
                if len(l.strip()) > 0:
                    yield json.loads(l)

    def _format_time_for_export(self, dt):
        try:
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except AttributeError:
            return dt

    def subscriber_activity_export_api_v3(self, campaign_id, **kwargs):
        campaign_meta = self.campaigns.get(campaign_id=campaign_id)
        list_id = campaign_meta['recipients']['list_id']
        since = self._format_time_for_export(kwargs.pop('since', None))
        for item in self.subscriber_activity_export(campaign_id,
                                                    since=since,
                                                    **kwargs):
            if item.get('error'):
                code = item.get('code', '<Missing>')
                error = item['error']
                msg = 'Error Code {}: {}'.format(code, error)
                raise SubscriberActivityExportError(item['error'], {
                    'error': item['error'],
                    'code': item.get('code'),
                    'campaign_id': campaign_id,
                    'list_id': list_id,
                    'since': since,
                    'kwargs': kwargs
                })
            yield ApiVersionTool.coerce_activity_export_to_api_v3(campaign_id,
                                                                  list_id, item)

    def iter_all(self, endpoint, get_all=False, offset=0, **kwargs):
        api = self._api_object(endpoint)
        args = {**kwargs}
        if self.exclude_links:
            ef = set(args.get('exclude_fields', '').split(','))
            ef.add('_links')
            ef.add('{}._links'.format(self._coll_key(endpoint)))
            args['exclude_fields'] = ','.join(ef)
        while True:
            with Timer(Metric.http_request_duration,
                       {Tag.endpoint: endpoint,
                        'get_all': get_all,
                        'offset': offset,
                        **args}):
                response = api.all(offset=offset, get_all=get_all, **args)
            n = len(response[self._coll_key(endpoint)])
            offset += n
            yield response
            if get_all or n == 0:
                break

    def iter_items(self, endpoint, **kwargs):
        total_items = self.total_items(endpoint, **kwargs)
        with progress_counter(total_items, endpoint, tags=kwargs) as pc:
            for response in self.iter_all(endpoint, **kwargs):
                current_items = response[self._coll_key(endpoint)]
                yield from current_items
                pc.increment(len(current_items))

    def total_items(self, endpoint, **kwargs):
        api = self._api_object(endpoint)
        with Timer(Metric.http_request_duration,
                   {Tag.endpoint: endpoint,
                    'fields': 'total_items',
                    **kwargs}):
            response = api.all(fields='total_items', **kwargs)
        return response['total_items']

    def schema(self, endpoint='', fetch_refs=True):
        return self._get_schema(endpoint, False, fetch_refs)

    def item_schema(self, endpoint='', fetch_refs=True):
        return self._get_schema(endpoint, True, fetch_refs)

    def __getattr__(self, attr):
        return getattr(self._mc3, attr)

    def __str__(self):
        fmt = '{}({!r})'
        return fmt.format(type(self).__name__, self._user_name)

    def __repr__(self):
        fmt = '{}(user_name={!r}, api_key={!r}, timeout={!r}, request_headers={!r})'
        return fmt.format(type(self).__name__, self._user_name, self._api_key,
                          self._timeout, self._headers)

    def _get_schema(self, endpoint, item_def, fetch_refs):
        url = self._schema_url(endpoint, item_def)
        with Timer(Metric.http_request_duration,
                   {Tag.endpoint: endpoint,
                    'url': url,
                    'action': 'get_schema',
                    'item_def': item_def,
                    'fetch_refs': fetch_refs}):
            schema = self._get(url).json()
            if fetch_refs:
                walk(schema, self._fill_in_refs)
        return schema

    def _schema_url(self, endpoint, item_def):
        if endpoint == '':
            return '{base}/Root.json'.format(base=self._schema_base)
        # path: lists.members -> Lists/Members,
        #       reports.email_activity -> Reports/EmailActivity
        path = '/'.join([''.join([f[0].upper() + f[1:]
                                  for f in p.split('_')])
                         for p in endpoint.split('.')])
        json_target = 'Instance' if item_def else 'Collection'
        return '{base}/{path}/{target}.json'.format(base=self._schema_base,
                                                    path=path,
                                                    target=json_target)

    @property
    def _schema_base(self):
        return 'https://{}/schema/3.0'.format(self._base_loc)

    @property
    def _export_base(self):
        return 'https://{}/export/1.0'.format(self._base_loc)

    @property
    def _base_loc(self):
        return urlparse(self._mc3.base_url).netloc

    def _get(self, url):
        return requests.get(url, timeout=self._timeout, headers=self._headers)

    def _api_object(self, endpoint):
        obj = self._mc3
        for p in endpoint.split('.'):
            obj = getattr(obj, p)
        return obj

    def _fill_in_refs(self, obj):
        try:
            url = obj['$ref']
            json = self._get(url).json()
            obj.update(json)
        except (TypeError, KeyError):
            pass

    _coll_key_map = {'reports.email_activity': 'emails'}

    @classmethod
    def _coll_key(cls, endpoint):
        return cls._coll_key_map.setdefault(endpoint, endpoint.split('.')[-1])


class ApiVersionTool:
    LIST_EXPORT_V1_TO_API_V3 = {
        'CC': {'v3_key': 'location.country_code',
               'coerce': str},
        'CONFIRM_IP': {'v3_key': 'ip_opt',
                       'coerce': str},
        'CONFIRM_TIME': {'v3_key': 'timestamp_opt',
                         'coerce': datify},
        'DSTOFF': {'v3_key': 'location.dstoff',
                   'coerce': int_or_float},
        'EUID': {'v3_key': 'unique_email_id',
                 'coerce': str},
        'Email Address': {'v3_key': 'email_address',
                          'coerce': str},
        'GMTOFF': {'v3_key': 'location.gmtoff',
                   'coerce': int_or_float},
        'LAST_CHANGED': {'v3_key': 'last_changed',
                         'coerce': datify},
        'LATITUDE': {'v3_key': 'location.latitude',
                     'coerce': int_or_float},
        'LEID': None,
        'LONGITUDE': {'v3_key': 'location.longitude',
                      'coerce': int_or_float},
        'MEMBER_RATING': {'v3_key': 'member_rating',
                          'coerce': int_or_float},
        'NOTES': None,
        'OPTIN_IP': {'v3_key': 'ip_signup',
                     'coerce': str},
        'OPTIN_TIME': {'v3_key': 'timestamp_signup',
                       'coerce': datify},
        'REGION': None,
        'TIMEZONE': {'v3_key': 'location.timezone',
                     'coerce': str}
    }

    @classmethod
    def api_v3_map_with_merge_fields(cls, merge_fields):
        mappings = dict(cls.LIST_EXPORT_V1_TO_API_V3)
        fmt = 'merge_fields.{}'
        for field in merge_fields:
            type_, name, tag = field['type'], field['name'], field['tag']
            mappings[name] = {'v3_key': fmt.format(tag)}
            if type_ == 'number':
                mappings[name]['coerce'] = int_or_float
            elif type_ == 'date':
                mappings[name]['coerce'] = datify_or_none
            else:
                mappings[name]['coerce'] = str
        logger.info({
            'description': 'No API v3 mapping available for list export columns',
            'columns': [k for k, v in mappings.items() if v is None]
        })
        # Return valid mappings
        return {k: v for k, v in mappings.items() if v is not None}

    @staticmethod
    def coerce_list_export_to_api_v3(mappings, list_id, status, export_data):
        email_address = export_data['Email Address']
        api_v3_data = {'id': mailchimp_email_id(email_address),
                       'list_id': list_id,
                       'status': status}
        for old_key, xform in mappings.items():
            old_value = export_data.get(old_key)
            if old_value is None or old_value == '':
                continue
            new_key, coerce = xform['v3_key'], xform['coerce']
            try:
                new_value = coerce(old_value)
            except Exception as e:
                # Add some execution context to the error.
                ctx = {'error': str(e),
                       'error_type': type(e).__name__,
                       'key': old_key,
                       'value': old_value,
                       'type': type(old_value).__name__,
                       'coerce': coerce.__name__}
                raise e.__class__(json.dumps(ctx)) from e
            set_deep(api_v3_data, new_key, new_value, sep='.')
        return api_v3_data

    @staticmethod
    def coerce_activity_export_to_api_v3(campaign_id, list_id, export_data):
        if not isinstance(export_data, dict):
            fmt = 'dict expected (got {}: {})'
            msg = fmt.format(type(export_data).__name__, export_data)
            raise ValueError(msg)
        if len(list(export_data.keys())) > 1:
            fmt = 'One key expected (got {}) in {}'
            msg = fmt.format(len(list(export_data.keys())), export_data)
            raise ValueError(msg)
        d = {}
        for email_address, activity_list in export_data.items():
            if not isinstance(activity_list, list):
                fmt = 'list expected (got {}: {}) in {}'
                msg = fmt.format(type(activity_list).__name__,
                                 activity_list,
                                 export_data)
                raise ValueError(msg)
            d['campaign_id'] = campaign_id
            d['email_address'] = email_address
            d['email_id'] = mailchimp_email_id(email_address)
            d['list_id'] = list_id
            d['activity'] = []
            for activity in activity_list:
                a = {'action': activity['action'],
                     'ip': activity['ip'],
                     'timestamp': datify(activity['timestamp'])}
                d['activity'].append(a)
        return d
