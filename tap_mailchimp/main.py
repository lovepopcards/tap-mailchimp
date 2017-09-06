"""Entry point for tap-mailchimp."""

from singer.utils import parse_args
import tap_mailchimp.config as tapconfig
from tap_mailchimp.tap import MailChimpTap

REQUIRED_KEYS = ['username', 'api_key', 'start_date']

def main():
    """Entry point for tap-mailchimp."""
    args = parse_args(REQUIRED_KEYS)
    tap = MailChimpTap(args.config, args.state)
    if args.discover:
        raise NotImplementedError('Discovery not yet implemented.')
    elif args.catalog is not None:
        raise NotImplementedError('Catalog support not yet implemented.')
    elif args.properties is not None:
        raise NotImplementedError('Properties support not yet implemented.')
    else:
        tap.pour()
    return 0
