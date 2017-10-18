"""Utility functions for Singer.io Mailchimp tap."""

from collections import abc, deque
import dateutil
import hashlib
from singer import Schema
import tap_mailchimp.logger as logger


def set_deep(dict_, key, value, sep='.'):
    if isinstance(key, str):
        key = key.split(sep) if sep is not None else [key]
    *path, leaf = key
    d = dict_
    for k in path:
        d = d.setdefault(k, {})
    d[leaf] = value


def clean_links(d):
    clean_keys(d, ['_links'])


def clean_keys(d, ks):
    def delkeys(obj):
        for k in ks:
            try:
                del obj[k]
            except (TypeError, KeyError):
                pass
    walk(d, delkeys)


def walk(obj, visit, strategy='DFS'):
    """Walk every node in a tree-like object and apply visit procedure to each.

    Args:
        obj (object): Tree-like object to walk. In most cases this will be a
            dict, e.g. from a json object.
        visit (callable): Method to call on each node.
        strategy (str): One of 'DFS' (depth-first search) or 'BFS'
            (breadth-first search). Optional, default is 'DFS'.
    """
    if strategy == 'DFS':
        nodes_to_walk = list()
        getnode = nodes_to_walk.pop
    elif strategy == 'BFS':
        nodes_to_walk = deque()
        getnode = nodes_to_walk.popleft
    else:
        raise ValueError(strategy)
    nodes_to_walk.append(obj)
    while len(nodes_to_walk) > 0:
        node = getnode()
        visit(node)
        if isinstance(node, abc.Mapping):
            nodes_to_walk.extend(node.values())
        elif isinstance(node, abc.Sequence) and not isinstance(node, str):
            nodes_to_walk.extend(node)


def fix_blank_date_time_format(schema, json):
    """
    Fix date-time formatted types in JSON data.

    Empty strings won't validate against the date-time format so delete them
    from date-time properties.

    Args:
        json_schema (singer.Schema): Singer JSON schema object.
        json (dict): JSON data object.

    Raises:
        ValueError: Exceptions are augmented with contextual processing
            information and re-raised as ValueError. This can happen if the JSON
            data is malformed, for example.
    """
    stack_to_process = list()
    stack_to_process.append((schema, json))
    while len(stack_to_process) > 0:
        s, j = stack_to_process.pop()
        try:
            if s.type == 'object':
                if s.properties is None:
                    # NOTE: Some MailChimp schemata use additionalProperties
                    # instead of properties, which the singer Schema class does not
                    # support. I think this means some MailChimp date-times are
                    # inappropriately coming through as strings but have not
                    # investigated further.
                    continue
                for prop, spec in s.properties.items():
                    if prop not in j:
                        continue
                    if spec.type in ('object', 'array'):
                        stack_to_process.append((spec, j[prop]))
                    elif spec.type == 'string':
                        if spec.format == 'date-time':
                            if j[prop] == '':
                                # Remove empty date-time property
                                del j[prop]
            elif s.type == 'array':
                if s.items is None:
                    # Skip because no item definition in schemata.
                    continue
                if s.items.type in ('object', 'array'):
                    stack_to_process.extend([(s.items, datum) for datum in j])
                elif s.items.type == 'string':
                    if s.items.format == 'date-time':
                        j[:] = [datum for datum in j if datum != '']
        except (TypeError, ValueError, LookupError) as e:
            # Augment with contextual info
            raise ValueError({'stack': stack_to_process,
                              'current': (s, j),
                              'schema': schema,
                              'json': json}) from e


def datify(dt):
    if dt is None or dt == '':
        return None
    dtobj = dateutil.parser.parse(dt)
    if dtobj.tzinfo is None:
        return dtobj.replace(tzinfo=dateutil.tz.tzutc()).isoformat()
    else:
        return dtobj.isoformat()


def datify_or_none(dt):
    try:
        return datify(dt)
    except ValueError as e:
        logger.exception(e, action='replace', old_value=dt, new_value=None)
        return None


def int_or_float(v):
    try:
        return int(v)
    except ValueError:
        return float(v)


def mailchimp_email_id(email_address):
    hasher = hashlib.md5()
    hasher.update(email_address.lower().encode('utf-8'))
    return hasher.hexdigest()


def tap_start_date(config, state):
    return state.last_run or config.start_date


def roundrobin(*iterables):
    """roundrobin('ABC', 'D', 'EF') --> A D E B F C"""
    # From https://docs.python.org/3/library/itertools.html
    # Recipe credited to George Sakkis
    pending = len(iterables)
    nexts = cycle(iter(it).__next__ for it in iterables)
    while pending:
        try:
            for next in nexts:
                yield next()
        except StopIteration:
            pending -= 1
            nexts = cycle(islice(nexts, pending))
