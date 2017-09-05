"""Useful functions for testing MailChimp tap.

Usage:
    >>> import tap_mailchimp.test as t
    >>> catalog = t.test_discover()
    >>> lists = t.test_lists(count=10)
"""

import os
import json
import singer
import tap_mailchimp.tap as tap
import tap_mailchimp.config as cfg

def project_path():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))

def config_path():
    return os.path.join(project_path(), 'config.json')

def state_path():
    return os.path.join(project_path(), 'state.json')

def catalog_path():
    return os.path.join(project_path(), 'catalog.json')

def loadargs():
    with open(config_path(), 'r') as f:
        config = json.load(f)
    with open(state_path(), 'r') as f:
        state = json.load(f)
    catalog = None
    if os.path.exists(catalog_path()):
        with open(catalog_path(), 'r') as f:
            catalog = singer.Catalog.from_dict(json.load(f))
    return (config, state, catalog)

def init_tap():
    config, state, catalog = loadargs()
    return tap.MailChimpTap(config, state, catalog)

def test_discover():
    t = init_tap()
    return t.discover()

def test_pour_all_lists():
    t = init_tap()
    t.config[cfg.start_date] = '*'
    t.pour_lists()

def test_pour_config_lists():
    pass

def test_pour_bookmark_lists():
    pass
