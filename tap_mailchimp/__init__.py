"""Mailchimp tap for singer.io.

Usage:

Discover available streams:

    $ tap-mailchimp -c config.json --discover

Save catalog:

    $ tap-mailchimp -c config.json --discover > catalog.json

Tap data:

    $ tap-mailchimp -c config.json [--catalog catalog.json] [--state state.json]

By default all data is tapped.
"""

from tap_mailchimp.tap import MailChimpTap
from tap_mailchimp.main import main
