"""Mailchimp tap for singer.io.

Usage:

Tap data:

    $ tap-mailchimp -c config.json [--catalog catalog.json] [--state state.json]

By default all data is tapped.
"""

from tap_mailchimp.tap import MailChimpTap
from tap_mailchimp.main import main
