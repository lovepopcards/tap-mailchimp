=============
tap-mailchimp
=============

This is a Singer (<https://singer.io>) tap that produces JSON-formatted data
following the Singer spec
(<https://github.com/singer-io/getting-started/blob/master/SPEC.md>).

This tap:

* Pulls raw data from the Mailchimp API
  (<http://developer.mailchimp.com/documentation/mailchimp/reference/overview/>)

* Extracts the following resources from Mailchimp:

    - lists

    - list members

    - campaigns

    - reports

        - email activity

* Outputs the schema for each resource

* Incrementally pulls data based on the input state

Supported streams:

* lists

* list_members

* campaigns

* email_activity_reports

Quick start
===========

Install
-------

Install from Github with pip::

    pip install git+https://github.com/lovepopcards/tap-mailchimp.git

Get an API key
--------------

The Tap will need to use a Mailchimp username and API key to make authenticated
requests to the Mailchimp API.

Create the config file
----------------------

The following is an example of the required configuration::

    {
        "start_date": "2017-09-01T00:00:00Z",
        "user_name": "email@example.com",
        "api_key": "abcdef12345-usn"
    }

A ``start_date`` of blank or ``*`` will pull all data (full historical import).
This is slow and not recommended.

Optional: Create the initial state file
---------------------------------------

You can provide a JSON file that contains stream progress data. This allows the
Tap to restart without losing too much progress. The Tap periodically (currently
every 60 seconds) emits a state JSON that you or the target should save to
support restarts. If you omit the state file or the state file is an empty JSON
object then the Tap will fetch all data for the supported streams.

Run the Tap
-----------

Run the tap with configuration and state::

    tap-mailchimp -c config.json -s state.json

Configuration options
=====================

* ``user_name``: MailChimp user name, required

* ``api_key``: MailChimp API key, required

* ``user_agent``: User agent for API requests, recommended

* ``start_date``: Starting timestamp for list members and campaign activity.
  ``"*"``, empty string, null, and not present are also accepted and mean no
  starting date (full historical import). Recommended.

* ``lag``: Lag in days for campaign reporting. Optional, default is 3.

* ``count``: Number of records to fetch at once through the API. Optional,
  default is 500.

* ``request_timeout``: Seconds before request times out. Optional, default is
  300 (5 minutes).

* ``max_run_time``: Minutes to run before exiting early. Useful for e.g. hourly
  jobs. Optional, default is null (no early exit).

* ``keep_links``: If true, ``_links`` from the API response are preserved. These
  are generally not useful. Optional, default is false.

* ``use_export``: If true, the MailChimp bulk export v1 API is used for list
  members and email activity. Highly recommended, default is true.

* ``use_list_member_export``: If true, use bulk export for list members. Default
  is to fallback to value of ``use_export``.

* ``use_email_activity_export``: If true, use bulk export for email activity.
  Default is to fallback to value of ``use_export``.

* ``include_empty_activity``: If true, include empty activity when tapping email
  activity stream. Optional, default is false.

* ``interests_array``: If true, convert interests to an array rather than an
  object. This results in a list member interests subtable. Optional, default is
  true.

* ``merge_fields_array``: If true, convert merge fields to an array rather than
  an object. This results in a list member merge fields subtable. Optional,
  default is true.

----

Copyright (C) 2017 Lovepop, LLC
