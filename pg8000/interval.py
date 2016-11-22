#!/usr/bin/env python

# Copyright (c) 2007-2016, Mathieu Fenniak
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
# * Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
# * The name of the author may not be used to endorse or promote products
# derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.


class Interval(object):
    """An Interval represents a measurement of time.  In PostgreSQL, an
    interval is defined in the measure of months, days, and microseconds; as
    such, the pg8000 interval type represents the same information.

    Note that values of the :attr:`microseconds`, :attr:`days` and
    :attr:`months` properties are independently measured and cannot be
    converted to each other.  A month may be 28, 29, 30, or 31 days, and a day
    may occasionally be lengthened slightly by a leap second.

    .. attribute:: microseconds

        Measure of microseconds in the interval.

        The microseconds value is constrained to fit into a signed 64-bit
        integer.  Any attempt to set a value too large or too small will result
        in an OverflowError being raised.

    .. attribute:: days

        Measure of days in the interval.

        The days value is constrained to fit into a signed 32-bit integer.
        Any attempt to set a value too large or too small will result in an
        OverflowError being raised.

    .. attribute:: months

        Measure of months in the interval.

        The months value is constrained to fit into a signed 32-bit integer.
        Any attempt to set a value too large or too small will result in an
        OverflowError being raised.
    """

    min_int4 = -2 ** 31
    max_int4 =  2 ** 31
    min_int8 = -2 ** 63
    max_int8 =  2 ** 63

    def __init__(self, microseconds=0, days=0, months=0):
        self.microseconds = microseconds
        self.days = days
        self.months = months

    def _setMicroseconds(self, value):
        if not isinstance(value, integer_types):
            raise TypeError("microseconds must be an integer type")
        elif not (Interval.min_int8 < value < Interval.max_int8):
            raise OverflowError(
                "microseconds must be representable as a 64-bit integer")
        else:
            self._microseconds = value

    def _setDays(self, value):
        if not isinstance(value, integer_types):
            raise TypeError("days must be an integer type")
        elif not (Interval.min_int4 < value < Interval.max_int4):
            raise OverflowError(
                "days must be representable as a 32-bit integer")
        else:
            self._days = value

    def _setMonths(self, value):
        if not isinstance(value, integer_types):
            raise TypeError("months must be an integer type")
        elif not (Interval.min_int4 < value < Interval.max_int4):
            raise OverflowError(
                "months must be representable as a 32-bit integer")
        else:
            self._months = value

    microseconds = property(lambda self: self._microseconds, _setMicroseconds)
    days = property(lambda self: self._days, _setDays)
    months = property(lambda self: self._months, _setMonths)

    def __repr__(self):
        return "<Interval %s months %s days %s microseconds>" % (
            self.months, self.days, self.microseconds)

    def __eq__(self, other):
        return other is not None and isinstance(other, Interval) and \
            self.months == other.months and self.days == other.days and \
            self.microseconds == other.microseconds

    def __neq__(self, other):
        return not self.__eq__(other)
