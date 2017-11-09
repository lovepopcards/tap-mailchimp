import datetime
import itertools
from abc import abstractmethod
from singer import record_counter, Counter, Schema, write_schema, write_record
from singer.metrics import Metric, Tag
import tap_mailchimp.logger as logger
from .client import Status
from .utils import clean_links, fix_blank_date_time_format, tap_start_date


class Stream:
    lists = 'lists'
    list_members = 'list_members'
    campaigns = 'campaigns'
    email_activity_reports = 'email_activity_reports'
    api = {lists: lists,
           list_members: 'lists.members',
           campaigns: campaigns,
           email_activity_reports: 'reports.email_activity'}
    supported_streams = (lists, list_members, campaigns, email_activity_reports)
    _available = (lists, list_members, campaigns, email_activity_reports, api)


class TapStream:
    key_properties = 'id'

    def __init__(self, client, stream_id, config, state):
        self.stream_id = stream_id
        self._client = client
        self._config = config
        self._state = state
        self._schema = None

    @property
    def is_done(self):
        return self._state.get_done(self.stream_id)

    @property
    def _offset(self):
        return self._state.get_count(self.stream_id)

    @property
    def api(self):
        return Stream.api[self.stream_id]

    @property
    def schema(self):
        if self._schema is None:
            self._schema = Schema.from_dict(self._client.item_schema(self.api))
        return self._schema

    def _set_done(self):
        self._state.set_done(self.stream_id, True)

    def pour(self):
        self.pre_pour()
        self.pour_schema()
        with self._record_counter() as counter:
            for record in self._iter_records():
                self._write_record(record)
                self._update_state(record)
                self._state.sync()
                counter.increment()
        self.post_pour()

    def _record_counter(self):
        return record_counter(endpoint=self.stream_id)

    def _update_state(self, record):
        self._state.add_id(self.stream_id, record['id'])
        self._state.set_count(self.stream_id,
                              len(self._state.get_ids(self.stream_id)))

    def _iter_records(self):
        yield from self._client.iter_items(self.api,
                                           count=self._config.count,
                                           offset=self._offset)

    def pour_schema(self):
        write_schema(self.stream_id,
                     self.schema.to_dict(),
                     key_properties=self.key_properties)

    def pre_pour(self):
        self._log_start(offset=self._offset)
        self._state.currently_syncing = self.stream_id

    def post_pour(self):
        self._set_done()
        self._state.currently_syncing = None
        self._state.sync(force=True)
        self._log_finish()

    def log_skip(self, reason):
        self._log_info(action='skip', reason=reason)

    def __str__(self):
        return '{}({})'.format(type(self).__name__, self.stream_id)

    def __repr__(self):
        fmt = '{}(client={!r}, stream_id={!r}, state={!r})'
        return fmt.format(type(self).__name__, self._client, self.stream_id,
                          self._state)

    def __json__(self):
        return {'stream_id': self.stream_id}

    def _log_info(self, **tags):
        logger.info({'stream_id': self.stream_id, **tags})

    def _log_start(self, offset=0):
        self._log_info(action='start', offset=offset)

    def _log_finish(self):
        self._log_info(action='finish')

    @property
    def _start_date(self):
        return tap_start_date(self._config, self._state)

    @property
    def _lag_date(self):
        if self._start_date is None:
            return None
        return self._start_date - datetime.timedelta(days=self._config.lag)

    def _write_record(self, record):
        if not self._config.keep_links:
            clean_links(record)
        fix_blank_date_time_format(self._schema, record)
        write_record(self.stream_id, record)

class TapItemStream(TapStream):
    def __init__(self, client, stream_id, item_id, config, state):
        self.item_id = item_id
        super().__init__(client, stream_id, config, state)

    @property
    def is_done(self):
        return self._state.get_id_done(self.stream_id, self.item_id)

    @property
    def _offset(self):
        return self._state.get_id_count(self.stream_id, self.item_id)

    @abstractmethod
    def _iter_records(self):
        raise NotImplementedError()

    def _record_counter(self):
        return Counter(Metric.record_count, tags={Tag.endpoint: self.stream_id,
                                                  'item_id': self.item_id})

    def _update_state(self, record):
        old_count = self._state.get_id_count(self.stream_id, self.item_id)
        self._state.set_id_count(self.stream_id, self.item_id, 1 + old_count)

    def _set_done(self):
        self._state.set_id_done(self.stream_id, self.item_id, True)

    def __str__(self):
        return '{}({}, {})'.format(type(self).__name__, self.stream_id,
                                   self.item_id)

    def __repr__(self):
        fmt = '{}(client={!r}, stream_id={!r}, item_id={!r}, state={!r})'
        return fmt.format(type(self).__name__, self._client, self.stream_id,
                          self.item_id, self._state)

    def __json__(self):
        return {'stream_id': self.stream_id, 'item_id': self.item_id}

    def _log_info(self, **tags):
        super()._log_info(item_id=self.item_id, **tags)

class ListStream(TapStream):
    def __init__(self, client, config, state):
        super().__init__(client, Stream.lists, config, state)

class CampaignStream(TapStream):
    def __init__(self, client, config, state):
        super().__init__(client, Stream.campaigns, config, state)

    def _iter_records(self):
        if self._lag_date is None:
            yield from self._client.iter_items(
                self.api,
                count=self._config.count
            )
        else:
            gen_create = self._client.iter_items(
                self.api,
                count=self._config.count,
                since_create_time=self._start_date.isoformat()
            )
            gen_send = self._client.iter_items(
                self.api,
                count=self._config.count,
                since_send_time=self._lag_date.isoformat()
            )
            yield from itertools.chain(gen_create, gen_send)

class ListMemberStream(TapItemStream):
    def __init__(self, client, list_id, config, state):
        super().__init__(client, Stream.list_members, list_id, config, state)
        self._merge_fields = None

    @property
    def schema(self):
        if self._schema is None:
            raw_schema = self._client.item_schema(self.api)
            if self._config.merge_fields_array:
                # Replace merge fields object with array to make a separate table.
                mf_desc = raw_schema['properties']['merge_fields']['description']
                raw_schema['properties']['merge_fields'] = {
                    'description': mf_desc,
                    'type': 'array',
                    'items': {'type': 'object',
                              'properties': {'merge_id': {'type': 'number'},
                                             'tag': {'type': 'string'},
                                             'name': {'type': 'string'},
                                             'type': {'type': 'string'},
                                             'value': {'type': 'string'}}}
                }
            if self._config.interests_array:
                # Replace interest object with array to make a separate table.
                int_desc = raw_schema['properties']['interests']['description']
                raw_schema['properties']['interests'] = {
                    'description': int_desc,
                    'type': 'array',
                    'items': {'type': 'object'}
                }
            self._schema = Schema.from_dict(raw_schema)
        return self._schema

    def _iter_records(self):
        args = {}
        if self._config.use_list_member_export:
            # Bulk Export API
            if self._start_date is not None:
                args['since'] = self._start_date
            iterables = []
            for status in Status._available:
                yield from self._client.list_export_api_v3(list_id=self.item_id,
                                                           status=status,
                                                           **args)
        else:
            # API v3
            if self._start_date is not None:
                args['since_last_changed'] = self._start_date.isoformat()
            yield from self._client.iter_items(self.api,
                                               list_id=self.item_id,
                                               count=self._config.count,
                                               offset=self._offset)

    def _get_merge_fields(self):
        if self._merge_fields is None:
            mf_response = self._client.lists.merge_fields.all(
                list_id=self.item_id,
                get_all=True,
                fields=','.join(('merge_fields.merge_id',
                                 'merge_fields.name',
                                 'merge_fields.tag',
                                 'merge_fields.type'))
            )
            self._merge_fields = {field_spec['tag']: field_spec
                                  for field_spec in mf_response['merge_fields']}
        return self._merge_fields

    def _convert_merge_fields(self, record):
        mf_lookup = self._get_merge_fields()
        mf_dict = record.get('merge_fields', {})
        mf_list = []
        for tag, value in mf_dict.items():
            mf_list.append({'merge_id': mf_lookup[tag]['merge_id'],
                            'tag': tag,
                            'name': mf_lookup[tag]['name'],
                            'type': mf_lookup[tag]['type'],
                            'value': str(value)})
        record['merge_fields'] = mf_list

    def _convert_interests(self, record):
        interest_dict = record.get('interests', {})
        interest_list = []
        for interest_id, interest_value in record.get('interests', {}).items():
            interest_list.append({'id': interest_id, 'value': interest_value})
        record['interests'] = interest_list

    def _write_record(self, record):
        if self._config.merge_fields_array:
            self._convert_merge_fields(record)
        if self._config.interests_array:
            self._convert_interests(record)
        super()._write_record(record)

class EmailActivityStream(TapItemStream):
    key_properties = ['campaign_id', 'email_id']

    def __init__(self, client, campaign_id, config, state):
        super().__init__(client=client,
                         stream_id=Stream.email_activity_reports,
                         item_id=campaign_id,
                         config=config,
                         state=state)

    def _iter_records(self):
        args = {}
        if self._config.use_email_activity_export:
            # Bulk Export API
            if self._start_date is not None:
                args['since'] = self._start_date
            yield from self._client.subscriber_activity_export_api_v3(
                campaign_id=self.item_id,
                include_empty=self._config.include_empty_activity,
                **args
            )
        else:
            # API v3
            yield from self._client.iter_items(self.api,
                                               campaign_id=self.item_id,
                                               count=self._config.count,
                                               offset=self._offset)
