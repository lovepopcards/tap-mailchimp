"""Utility functions for Singer.io Mailchimp tap."""

from collections import abc
from functools import lru_cache
import requests
from singer import http_request_timer


def pluck(dict_, *keys):
    """Return iterable of values for given keys in dict_"""
    return (dict_[k] for k in keys)


def schema_url_from_path(path):
    return 'https://us3.api.mailchimp.com/schema/3.0/Definitions/{}/Response.json'.format(path)


SCHEMA_URL = {'lists': schema_url_from_path('Lists'),
              'list_members': schema_url_from_path('Lists/Members'),
              'campaigns': schema_url_from_path('Campaigns'),
              'email_activity_reports': schema_url_from_path('Reports/EmailActivity')}


@lru_cache(maxsize=128, typed=False)
def download_schema(schema_url):
    """Get schema located at schema_url. Schemata are cached.

    :param schema_url: URL with schema to fetch.
    :return: JSON Schema.
    :rtype: dict
    """
    with http_request_timer(endpoint="schemas"):
        return requests.get(schema_url).json()


@lru_cache(maxsize=128, typed=False)
def get_schema(stream):
    """Get schema for stream with given stream name.

    :param stream: Name of singer stream.
    :return: JSON Schema.
    :rtype: dict
    """
    schema = download_schema(SCHEMA_URL[stream])
    process_schema(schema)
    return schema


def process_schema(schema):
    """Apply cleanups to MailChimp JSON schema."""
    # Follow and fill in $refs. TODO Guard against infinite recursion.
    walk(schema, fill_in_refs)
    # Sometimes the datetimes are just empty strings, which won't validate.
    walk(schema, fix_date_time_format)


def fill_in_refs(node):
    """Download and fill in $ref schemas for JSON schema node."""
    try:
        url = node['$ref']
        node.update(download_schema(url))
    except (TypeError, KeyError):
        pass


def fix_date_time_format(node):
    """Fix date-time formatted types in JSON schema node.

    Sometimes the datetimes are just empty strings, which won't validate. Here
    we also allow zero-length strings.

    :param node: Node in JSON schema.
    """
    try:
        if node['format'] == 'date-time' and isinstance(node['type'], str):
            del node['format']
            del node['type']
            node['anyOf'] = [{'type': ['null', 'string'], 'format': 'date-time'},
                             {'type': 'string', 'maxLength': 0}]
    except (TypeError, KeyError):
        pass


def walk(node, visit):
    """Walk every node in a JSON object and apply visit procedure to each."""
    visit(node)
    if isinstance(node, abc.Mapping):
        for v in node.values():
            walk(v, visit)
    elif isinstance(node, abc.Sequence) and not isinstance(node, str):
        for v in node:
            walk(v, visit)
