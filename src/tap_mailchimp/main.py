"""Entry point for tap-mailchimp."""

import singer.utils
from .config import TapConfig
from .state import TapState
from tap_mailchimp.tap import MailChimpTap

def main():
    """Entry point for tap-mailchimp."""
    args = singer.utils.parse_args(TapConfig.required_keys)
    cfg = TapConfig(args.config)
    state = TapState(args.state)
    tap = MailChimpTap(cfg, state)
    if args.discover:
        raise NotImplementedError('Discovery not yet implemented.')
    elif args.catalog is not None:
        raise NotImplementedError('Catalog support not yet implemented.')
    elif args.properties is not None:
        raise NotImplementedError('Properties support not yet implemented.')
    else:
        tap.pour()
    return 0
