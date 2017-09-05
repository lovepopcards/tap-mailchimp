"""Entry point for tap-mailchimp."""

from singer import Catalog
from singer.utils import parse_args
import tap_mailchimp.config as tapconfig
from tap_mailchimp.tap import MailChimpTap

def main():
    """Entry point for tap-mailchimp."""
    args = parse_args(tapconfig.required_keys)
    tap = MailChimpTap(args.config, args.state)
    if args.discover:
        catalog = tap.discover()
        catalog.dump()
    elif args.catalog is not None:
        tap.catalog = args.catalog
        tap.pour()
    elif args.properties is not None:
        tap.catalog = Catalog.from_dict(args.properties)
        tap.pour()
    else:
        tap.pour()
    return 0
