"""Utility functions for Singer.io Mailchimp tap."""

from collections import abc
from functools import lru_cache
import requests
from singer import http_request_timer
import tap_mailchimp.config as tapconfig

def pluck(dict_, *keys):
    """Return iterable of values for given keys in dict_"""
    return (dict_[k] for k in keys)

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
    schema = download_schema(tapconfig.schema_url[stream])
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
    we replace type with ["null", "string"]. Then in the data we have to
    replace empty strings with nulls.

    :param node: Node in JSON schema.
    """
    try:
        # TODO Update singer.schema to support anyOf. Until then we don't have a
        #      way of accepting both date-times and empty strings.
        # if node['format'] == 'date-time' and isinstance(node['type'], str):
            # del node['format']
            # del node['type']
            # node['anyOf'] = [{'type': ['null', 'string'], 'format': 'date-time'},
                             # {'type': 'string', 'maxLength': 0}]
        if node['format'] == 'date-time':
            del node['format']
    except (TypeError, KeyError):
        pass

def null_empty(node):
    """Change empty string to None.

    :param node: Node in JSON structure.
    """
    if isinstance(node, abc.Mapping):
        for k, v in node.items():
            if isinstance(v, str) and v == '':
                node[k] = None
    elif isinstance(node, abc.Sequence) and not isinstance(node, str):
        for idx, v in enumerate(node):
            if isinstance(v, str) and v == '':
                node[idx] = None

def walk(node, visit):
    """Walk every node in a JSON object and apply visit procedure to each."""
    visit(node)
    if isinstance(node, abc.Mapping):
        for v in node.values():
            walk(v, visit)
    elif isinstance(node, abc.Sequence) and not isinstance(node, str):
        for v in node:
            walk(v, visit)
