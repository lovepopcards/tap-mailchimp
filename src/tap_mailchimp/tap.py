"""MailChimpTap for singer.io."""

from collections import deque
from singer import job_timer
from .client import MailChimp
from .streams import (ListStream,
                      ListMemberStream,
                      CampaignStream,
                      EmailActivityStream,
                      Stream)
from .utils import roundrobin
import itertools
import tap_mailchimp.logger as logger


class MailChimpTap:
    """Singer.io tap for MailChimp API v3.

    Usage:
        >>> tap = MailChimpTap(config, state)
        >>> tap.pour()
    """

    def __init__(self, config, state):
        self.config = config
        self.state = state
        self.client = MailChimp(config.user_name,
                                config.api_key,
                                user_agent=config.user_agent,
                                timeout=config.request_timeout,
                                exclude_links=(not config.keep_links))

    def pour(self):
        """Pour schemata and data from the Mailchimp tap."""
        is_error = False
        stream_gens = []
        stream_gens.append(self.lists_stream_gen())
        stream_gens.append(self.campaigns_stream_gen())
        stream_gens.append(self.list_members_stream_gen())
        stream_gens.append(self.email_activity_reports_stream_gen())
        for stream in itertools.chain(*stream_gens):
            if self._check_stop():
                self._log_early_stop()
                return
            if stream.is_done:
                stream.log_skip('done')
                continue
            try:
                with job_timer(job_type=stream.stream_id):
                    stream.pour()
            except Exception as e:
                is_error = True
                logger.exception(e, stream=stream)
        if not is_error:
            self.state.finalize_run()
        self.state.sync(force=True)

    def lists_stream_gen(self):
        stream = ListStream(self.client, self.config, self.state)
        yield stream

    def list_members_stream_gen(self):
        for list_id in self.state.get_ids(Stream.lists):
            stream = ListMemberStream(self.client, list_id, self.config,
                                      self.state)
            yield stream

    def campaigns_stream_gen(self):
        stream = CampaignStream(self.client, self.config, self.state)
        yield stream

    def email_activity_reports_stream_gen(self):
        for campaign_id in self.state.get_ids(Stream.campaigns):
            stream = EmailActivityStream(self.client, campaign_id, self.config,
                                         self.state)
            yield stream

    def __repr__(self):
        return '{}(config={!r}, state={!r})'.format(type(self).__name__,
                                                    self.config, self.state)

    def _check_stop(self):
        if self.config.max_run_time:
            runtime_minutes = self.state.session_time() / 60
            return self.config.max_run_time < runtime_minutes
        return False

    def _log_early_stop(self):
        logger.info({'action': 'stop',
                     'reason': 'exceeded session time',
                     'time': self.state.session_time() / 60})
