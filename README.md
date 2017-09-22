# tap-facebook

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following
the
[Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from
  the
  [Mailchimp API](http://developer.mailchimp.com/documentation/mailchimp/reference/overview/)
- Extracts the following resources from Mailchimp:
  - lists
  - list members
  - campaigns
  - reports
    - email activity
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick start

### Install

This project has not yet been packaged for pip. Future work.

### Get an API key

The Tap will need to use a Mailchimp username and API key to make authenticated
requests to the Mailchimp API.

### Create the config file

The following is an example of the required configuration

```json
{
    "start_date": "2017-09-01T00:00:00Z",
    "username": "email@example.com",
    "api_key": "abcdef12345-usn"
}
```

A `start_date` of blank or `*` will pull all data (full historical import). This
is slow and not recommended.

### [Optional] Create the initial state file

You can provide a JSON file that contains stream progress data. This allows the
Tap to restart without losing too much progress. The Tap periodically (currently
every 60 seconds) emits a state JSON that you or the target should save to
support restarts. If you omit the state file or the state file is an empty JSON
object then the Tap will fetch all data for the supported streams.

### Run the Tap

`tap-mailchimp -c config.json -s state.json`

---

Copyright &copy; 2017 Lovepop, LLC
