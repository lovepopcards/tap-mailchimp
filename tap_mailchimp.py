#!/usr/bin/env python

"""Mailchimp tap for singer.io."""

import singer
import singer.utils
from mailchimp3 import MailChimp
from collections import namedtuple

REQUIRED_CONFIG_KEYS = ['username', 'api_key']
API_ROOT = 'https://us3.api.mailchimp.com/3.0'

def pluck(dict_, *keys):
    """Return iterable of values for given keys in dict_"""
    return (dict_[k] for k in keys)

def mailchimp_gen(base, attr, **kwargs):
    """Generator to iterate over mailchimp responses.

    Mailchimp returns a subset of total data, and uses an `offset` parameter to
    iterate. We fetch and cache the next chunk of data, then call the API as
    needed.

    Usage:
        for item in mailchimp_gen(client, 'lists'):
            process(item)

    Args:
        base (object): mailchimp3.MailChimp or API child object
        attr (str): name of child object to based the generator on
        **kwargs: Arguments passed on to the API object's `all` method
    """
    process_count, total_items = 0, 1
    api_method = getattr(base, attr)
    while process_count < total_items:
        with singer.Timer('http_request_duration', dict(endpoint=attr, **kwargs)):
            response = api_method.all(offset=process_count, **kwargs)
        items, total_items = pluck(response, attr, 'total_items')
        for i in items:
            yield i
            process_count += 1

class MailChimpTap:
    """Singer.io tap for MailChimp API v3.

    Usage:
    tap = MailChimpTap(config, state)
    tap.pour()

    Arguments:
    config -- Configuration object from singer.
    state -- State object from singer.
    """

    def __init__(self, config, state):
        self.start_date = state.get('last_processed_date',
                                    config.get('start_date',
                                               '2017-01-01T00:00:00Z'))
        self.client = MailChimp(config['username'], config['api_key'])

    def pour(self):
        with singer.job_timer(job_type='lists'):
            self.pour_root()
            # self.pour_lists(start_date)
    def pour_lists(self, start_date):
        with singer.record_counter(endpoint='lists') as list_counter:
            for list_ in mailchimp_gen(self._client, 'lists'):
                del list_['_links']
                del list_['stats']
                singer.write_record('lists', list_)
                list_counter.increment()
                self.pour_list_members(list_['id'], start_date)
    def pour_list_members(list_id, start_date):
        with singer.Counter('record_count',
                            {'endpoint': 'members',
                             'list_id': list_id}) as member_counter:
            for member in mailchimp_gen(self._client.lists, 'members', list_id=list_id):
                singer.write_record('members', member)
                member_counter.increment()
                self.pour_email_activity(member['id'], start_date)

def main():
    """Entry point for tap-mailchimp."""
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    tap = MailChimpTap(args.config, args.state)
    if args.discover:
        raise NotImplementedError('Catalog discovery is not yet supported.')
    elif args.catalog is not None:
        raise NotImplementedError('Catalogs are not yet supported.')
    elif args.properties is not None:
        raise NotImplementedError('Properties are not supported.')
    else:
        tap.pour()
    return 0

if __name__ == '__main__':
    main()
