#!/usr/bin/env python

import datetime

class UTC(datetime.tzinfo):
    zero_delta = datetime.timedelta(0)

    def utcoffset(self, dt):
        return UTC.zero_delta

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return UTC.zero_delta
