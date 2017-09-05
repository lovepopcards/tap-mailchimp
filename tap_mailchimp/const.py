"""Configurations and constants for Singer.io Mailchimp tap.

Interesting constants:

* required_keys: keys that are required from config JSON file
* all_streams: list of stream names
* schema_url: dict of stream names to schema URLs
"""

# Configuration
username = 'username'
api_key = 'api_key'
start_date = 'start_date'
user_agent = 'user_agent'
lag_days = 'lag_days'
count = 'count'
retries = 'retries'
validate = 'validate'

required_keys = (username, api_key, start_date)

# State
bookmarks = 'bookmarks'
last_record = 'last_record'

# Streams
streams = 'streams'
lists = 'lists'
list_members = 'list_members'
campaigns = 'campaigns'
email_activity_reports = 'email_activity_reports'

all_streams = [lists, list_members, campaigns, email_activity_reports]

_schema_url = 'schema_url'
_key_properties = 'key_properties'
_endpoint = 'endpoint'
_response_key = 'response_key'
_api_attr = 'api_attr'
_date_key = 'date_key'

def _schema_url_from_path(path):
    return 'https://us3.api.mailchimp.com/schema/3.0/Definitions/{}/Response.json'.format(path)

stream_defs = {
    lists: {
        _schema_url: _schema_url_from_path('Lists'),
        _key_properties: 'id',
        _date_key: 'since_campaign_last_sent'
    },
    list_members: {
        _schema_url: _schema_url_from_path('Lists/Members'),
        _key_properties: 'id',
        _endpoint: 'members',
        _api_attr: 'members',
        _date_key: 'since_last_changed'
    },
    campaigns: {
        _schema_url: _schema_url_from_path('Campaigns'),
        _key_properties: 'id',
        _date_key: 'since_send_time'
    },
    email_activity_reports: {
        _schema_url: _schema_url_from_path('Reports/EmailActivity'),
        _key_properties: ['campaign_id', 'email_id'],
        _endpoint: 'email-activity',
        _api_attr: 'email_activity'
    }
}

schema_url = {k: v[_schema_url] for k, v in stream_defs.items()}

key_properties = {k: v[_key_properties] for k, v in stream_defs.items()}

endpoint = {k: v.get(_endpoint, k) for k, v in stream_defs.items()}

api_attr = {k: v.get(_api_attr, k) for k, v in stream_defs.items()}

date_key = {k: v.get(_date_key) for k, v in stream_defs.items()}

