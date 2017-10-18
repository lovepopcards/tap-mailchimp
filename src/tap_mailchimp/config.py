import dateutil
import tap_mailchimp.logger as logger
from .jsonext import JsonObject


DEFAULT_START_DATE = '*'
DEFAULT_LAG = 3
DEFAULT_COUNT = 500
DEFAULT_REQUEST_TIMEOUT = 300
DEFAULT_MAX_RUN_TIME = None
DEFAULT_KEEP_LINKS = False
DEFAULT_USE_EXPORT = True
DEFAULT_INCLUDE_EMPTY_ACTIVITY = False
DEFAULT_INTERESTS_ARRAY = True
DEFAULT_MERGE_FIELDS_ARRAY = True
DEFAULT_USER_AGENT = 'singer.io:tap_mailchimp/alpha'


class Keys:
    user_name = 'user_name'
    api_key = 'api_key'
    user_agent = 'user_agent'
    start_date = 'start_date'
    lag = 'lag'
    count = 'count'
    request_timeout = 'request_timeout'
    max_run_time = 'max_run_time'
    keep_links = 'keep_links'
    use_export = 'use_export'
    use_list_member_export = 'use_list_member_export'
    use_email_activity_export = 'use_email_activity_export'
    include_empty_activity = 'include_empty_activity'
    interests_array = 'interests_array'
    merge_fields_array = 'merge_fields_array'
    test_mode = 'test_mode'


class TapConfig(JsonObject):

    required_keys = [Keys.user_name, Keys.api_key]

    defaults = {Keys.user_agent: DEFAULT_USER_AGENT,
                Keys.lag: DEFAULT_LAG,
                Keys.count: DEFAULT_COUNT,
                Keys.request_timeout: DEFAULT_REQUEST_TIMEOUT,
                Keys.max_run_time: DEFAULT_MAX_RUN_TIME,
                Keys.keep_links: DEFAULT_KEEP_LINKS,
                Keys.include_empty_activity: DEFAULT_INCLUDE_EMPTY_ACTIVITY,
                Keys.interests_array: DEFAULT_INTERESTS_ARRAY,
                Keys.merge_fields_array: DEFAULT_MERGE_FIELDS_ARRAY,
                Keys.test_mode: False}

    def __init__(self, cfg):
        super().__init__(cfg,
                         required_keys=self.required_keys,
                         defaults=self.defaults)
        self.start_date = self._parse_start_date(cfg.get(Keys.start_date,
                                                         DEFAULT_START_DATE))
        use_export = cfg.get(Keys.use_export, DEFAULT_USE_EXPORT)
        self.use_list_member_export = cfg.get(Keys.use_list_member_export,
                                              use_export)
        self.use_email_activity_export = cfg.get(Keys.use_email_activity_export,
                                                 use_export)

    @staticmethod
    def _parse_start_date(d):
        try:
            return dateutil.parser.parse(d)
        except (TypeError, ValueError):
            logger.debug({'action': 'replace',
                          'target': 'config.start_date',
                          'old': d,
                          'new': None})
            return None

