#!/usr/bin/env pypy

import six
import json
import uuid
import struct
import decimal
import datetime
import calendar
import ipaddress

from collections import defaultdict

from pg8000 import interval
from pg8000 import types
from pg8000 import constants
from pg8000 import errors
from pg8000 import tzutc


class PostgresTranslate(object):
    """
    Translate binary data received over the wire from Postgres into Python
    objects, and vice-versa.
    """

    def __init__(self, client_encoding="utf-8", max_pack_shorts_count=20):
        self.client_encoding = client_encoding

        self.FC_TEXT= constants.Constants.FC_TEXT      # 0
        self.FC_BINARY = constants.Constants.FC_BINARY # 1

        binary_formats = """i
                            h
                            q
                            d
                            f
                            iii
                            ii
                            qii
                            dii
                            ihihih
                            ci
                            bh
                            cccc""".split()
        for item in binary_formats:
            self._create_pack_funcs(item)

        self.max_pack_shorts_count = max_pack_shorts_count
        self._create_pack_shorts()

        self._create_bytea_recv_func()
        self._create_text_recv_func()

        self.utc = tzutc.UTC()
        self.epoch = datetime.datetime(2000, 1, 1)
        self.epoch_tz = self.epoch.replace(tzinfo=self.utc)
        self.epoch_seconds = calendar.timegm(self.epoch.timetuple())

        self.infinity_microseconds = 2 ** 63 - 1
        self.minus_infinity_microseconds = -1 * self.infinity_microseconds - 1

        self.datetime_max_tz = datetime.datetime.max.replace(tzinfo=self.utc)
        self.datetime_min_tz = datetime.datetime.min.replace(tzinfo=self.utc)

        self.true  = six.b("\x01")
        self.false = six.b("\x00")
        self.null  = self.i_pack(-1)
        self.null_byte = six.b("\x00")

        self.translation_table = dict(zip(map(ord, six.u('{}')), six.u('[]')))
        self.glbls = {"Decimal": decimal.Decimal}

        self._establish_postgres_types()
        self._establish_python_types()
        self._establish_encoding_hash()

    def _establish_postgres_types(self):
        """
        Connect the Postgres OID to the data type (text or binary) and the method we
        use to unpack that data
        """
        self.pg_types = defaultdict(
            lambda: (self.FC_TEXT, self.text_recv), {
                16:   (self.FC_BINARY, self.bool_recv),    # boolean
                17:   (self.FC_BINARY, self.bytea_recv),   # bytea
                19:   (self.FC_BINARY, self.text_recv),    # name type
                20:   (self.FC_BINARY, self.int8_recv),    # int8
                21:   (self.FC_BINARY, self.int2_recv),    # int2
                22:   (self.FC_TEXT,   self.vector_in),    # int2vector
                23:   (self.FC_BINARY, self.int4_recv),    # int4
                25:   (self.FC_BINARY, self.text_recv),    # TEXT type
                26:   (self.FC_TEXT,   self.int_in),       # oid
                28:   (self.FC_TEXT,   self.int_in),       # xid
                114:  (self.FC_TEXT,   self.json_in),      # json
                700:  (self.FC_BINARY, self.float4_recv),  # float4
                701:  (self.FC_BINARY, self.float8_recv),  # float8
                705:  (self.FC_BINARY, self.text_recv),    # unknown
                829:  (self.FC_TEXT,   self.text_recv),    # MACADDR type
                869:  (self.FC_TEXT,   self.inet_in),      # inet
                1000: (self.FC_BINARY, self.array_recv),   # BOOL[]
                1003: (self.FC_BINARY, self.array_recv),   # NAME[]
                1005: (self.FC_BINARY, self.array_recv),   # INT2[]
                1007: (self.FC_BINARY, self.array_recv),   # INT4[]
                1009: (self.FC_BINARY, self.array_recv),   # TEXT[]
                1014: (self.FC_BINARY, self.array_recv),   # CHAR[]
                1015: (self.FC_BINARY, self.array_recv),   # VARCHAR[]
                1016: (self.FC_BINARY, self.array_recv),   # INT8[]
                1021: (self.FC_BINARY, self.array_recv),   # FLOAT4[]
                1022: (self.FC_BINARY, self.array_recv),   # FLOAT8[]
                1042: (self.FC_BINARY, self.text_recv),    # CHAR type
                1043: (self.FC_BINARY, self.text_recv),    # VARCHAR type
                1082: (self.FC_TEXT,   self.date_in),      # date
                1083: (self.FC_TEXT,   self.time_in),
                1114: (self.FC_BINARY, self.timestamp_recv_float),  # timestamp w/ tz
                1184: (self.FC_BINARY, self.timestamptz_recv_float),
                1186: (self.FC_BINARY, self.interval_recv_integer),
                1231: (self.FC_TEXT,   self.num_array_in), # NUMERIC[]
                1263: (self.FC_BINARY, self.array_recv),   # cstring[]
                1700: (self.FC_TEXT,   self.numeric_in),   # NUMERIC
                2275: (self.FC_BINARY, self.text_recv),    # cstring
                2950: (self.FC_BINARY, self.uuid_recv),    # uuid
                3802: (self.FC_TEXT,   self.json_in),      # jsonb
            })
    def _establish_python_types(self):
        self.py_types = {
            type(None):            (-1,   self.FC_BINARY, self.null_send),  # null
            bool:                  (16,   self.FC_BINARY, self.bool_send),
            int:                   (705,  self.FC_TEXT,   self.unknown_out),
            float:                 (701,  self.FC_BINARY, self.d_pack),  # float8
            ipaddress.IPv4Address: (869,  self.FC_TEXT,   self.inet_out),  # inet
            ipaddress.IPv6Address: (869,  self.FC_TEXT,   self.inet_out),  # inet
            ipaddress.IPv4Network: (869,  self.FC_TEXT,   self.inet_out),  # inet
            ipaddress.IPv6Network: (869,  self.FC_TEXT,   self.inet_out),  # inet
            datetime.date:         (1082, self.FC_TEXT,   self.date_out),  # date
            datetime.time:         (1083, self.FC_TEXT,   self.time_out),  # time
            1114:                  (1114, self.FC_BINARY, self.timestamp_send_integer),  # timestamp
            # timestamp w/ tz
            1184:                  (1184, self.FC_BINARY, self.timestamptz_send_integer),
            datetime.timedelta:    (1186, self.FC_BINARY, self.interval_send_integer),
            interval.Interval:     (1186, self.FC_BINARY, self.interval_send_integer),
            decimal.Decimal:       (1700, self.FC_TEXT,   self.numeric_out),  # Decimal
            uuid.UUID:             (2950, self.FC_BINARY, self.uuid_send),  # uuid
        }

        if six.PY2:
            self.py_types[types.Bytea]   = (17,  self.FC_BINARY, self.bytea_send)  # bytea
            self.py_types[six.text_type] = (705, self.FC_TEXT,   self.text_out)  # unknown
            self.py_types[str]           = (705, self.FC_TEXT,   self.bytea_send)  # unknown
            self.py_types[long]          = (705, self.FC_TEXT,   self.unknown_out)  # noqa
        else:
            self.py_types[bytes] = (17, self.FC_BINARY, self.bytea_send)  # bytea
            self.py_types[str]   = (705, self.FC_TEXT, self.text_out)  # unknown


    def _establish_encoding_hash(self):
        """
        Translate a Postgres encoding name into the Python equivalent
        PostgreSQL encodings:
          http://www.postgresql.org/docs/8.3/interactive/multibyte.html
        Python encodings:
          http://www.python.org/doc/2.4/lib/standard-encodings.html
        
        Commented out encodings don't require a name change between PostgreSQL
        and Python.  If the py side is None, then the encoding isn't supported.
        """

        self.encodings = {
            # Not supported:
            "mule_internal": None,
            "euc_tw": None,

            # Name fine as-is:
            # "euc_jp",
            # "euc_jis_2004",
            # "euc_kr",
            # "gb18030",
            # "gbk",
            # "johab",
            # "sjis",
            # "shift_jis_2004",
            # "uhc",
            # "utf8",

            # Different name:
            "euc_cn": "gb2312",
            "iso_8859_5": "is8859_5",
            "iso_8859_6": "is8859_6",
            "iso_8859_7": "is8859_7",
            "iso_8859_8": "is8859_8",
            "koi8": "koi8_r",
            "latin1": "iso8859-1",
            "latin2": "iso8859_2",
            "latin3": "iso8859_3",
            "latin4": "iso8859_4",
            "latin5": "iso8859_9",
            "latin6": "iso8859_10",
            "latin7": "iso8859_13",
            "latin8": "iso8859_14",
            "latin9": "iso8859_15",
            "sql_ascii": "ascii",
            "win866": "cp886",
            "win874": "cp874",
            "win1250": "cp1250",
            "win1251": "cp1251",
            "win1252": "cp1252",
            "win1253": "cp1253",
            "win1254": "cp1254",
            "win1255": "cp1255",
            "win1256": "cp1256",
            "win1257": "cp1257",
            "win1258": "cp1258",
            "unicode": "utf-8",  # Needed for Amazon Redshift
        }


    def update_client_encoding(self, encoding):
        """
        Update our client encoding (e.g. 'utf-8'), normally based on the value
        sent to us by the server as part of a PARAMETER_STATUS message
        a datetime.datetime with no timezone data

        @param data: A binary-packed string representation of a 64-bit integer
        @type data: L{str}

        @param offset: Character offset into the data
        @type offset: L{int}

        @param length: Ignored
        @type length: L{int}

        @return: A datetime.datetime instance
        @rtype: L{datetime.datetime}
        """
        self.client_encoding = self.encodings.get(encoding, encoding)


    def timestamps_are_integers(self):
        self.pg_types[1114] = (self.FC_BINARY, self.timestamp_recv_integer)
        self.pg_types[1184] = (self.FC_BINARY, self.timestamptz_recv_integer)
        self.pg_types[1186] = (self.FC_BINARY, self.interval_recv_integer)

        self.py_types[1114] = (1114, self.FC_BINARY, self.timestamp_send_integer)
        self.py_types[1184] = (1184, self.FC_BINARY, self.timestamptz_send_integer)
        self.py_types[interval.Interval] = (1186, self.FC_BINARY, self.interval_send_integer)
        self.py_types[datetime.timedelta] = (1186, self.FC_BINARY, self.interval_send_integer)


    def timestamps_are_floats(self):
        self.pg_types[1114] = (self.FC_BINARY, self.timestamp_recv_float)
        self.pg_types[1184] = (self.FC_BINARY, self.timestamptz_recv_float)
        self.pg_types[1186] = (self.FC_BINARY, self.interval_recv_float)

        self.py_types[1114] = (1114, self.FC_BINARY, self.timestamp_send_float)
        self.py_types[1184] = (1184, self.FC_BINARY, self.timestamptz_send_float)
        self.py_types[interval.Interval] = (1186, self.FC_BINARY, self.interval_send_float)
        self.py_types[datetime.timedelta] = (1186, self.FC_BINARY, self.interval_send_float)


    def timestamp_recv_integer(self, data, offset, length):
        """
        Convert a binary-packed string representation of a 64-bit integer into
        a datetime.datetime with no timezone data

        @param data: A binary-packed string representation of a 64-bit integer
        @type data: L{str}

        @param offset: Character offset into the data
        @type offset: L{int}

        @param length: Ignored
        @type length: L{int}

        @return: A datetime.datetime instance
        @rtype: L{datetime.datetime}
        """
        micros = self.q_unpack(data, offset)[0]
        try:
            return self.epoch + datetime.timedelta(microseconds=micros)
        except OverflowError as e:
            if micros == self.infinity_microseconds:
                return datetime.datetime.max
            elif micros == self.minus_infinity_microseconds:
                return datetime.datetime.min
            else:
                raise e

    def timestamp_recv_float(self, data, offset, length):
        """
        Convert a binary-packed string representation of a double-precision
        float to a datetime.datetime with no timezone data

        @param data: A binary-packed string representation of a double
        @type data: L{str}

        @param offset: Character offset into the data
        @type offset: L{int}

        @param length: Ignored
        @type length: L{int}

        @return: A datetime.datetime instance
        @rtype: L{datetime.datetime}

        data is double-precision float representing seconds since 2000-01-01
        """
        return datetime.datetime.utcfromtimestamp(self.epoch_seconds + self.d_unpack(data, offset)[0])

    def timestamp_send_integer(self, timestamp):
        """
        Convert a datetime.datetime into a binary packed string representation
        of a 64-bit integer representing microseconds since 2000-01-01

        @param timestamp: A timestamp
        @type timestamp: L{datetime.datetime}

        @return: A Long, binary packed into an eight character string
        @rtype: L{str}
        """
        if timestamp == datetime.datetime.max:
            micros = self.infinity_microseconds
        elif timestamp == datetime.datetime.min:
            micros = self.minus_infinity_microseconds
        else:
            micros = int(
            (calendar.timegm(timestamp.timetuple()) - self.epoch_seconds) * 1e6) + timestamp.microsecond
        return self.q_pack(micros)

    def timestamp_send_float(self, timestamp):
        """
        Convert a datetime.datetime into a binary packed string representation
        of a double-precision float representing seconds since 2000-01-01

        @param timestamp: A timestamp
        @type timestamp: L{datetime.datetime}

        @return: A Double, binary packed into an eight character string
        @rtype: L{str}
        """
        return self.d_pack(calendar.timegm(timestamp.timetuple()) + timestamp.microsecond / 1e6 - self.epoch_seconds)

    def timestamptz_send_integer(self, timestamp):
        """
        Convert a datetime.datetime into a binary packed string representation
        of a 64-bit integer representing microseconds since 2000-01-01
        If the timestamp has timezone info, convert it to UTC.

        @param timestamp: A timestamp
        @type timestamp: L{datetime.datetime}

        @return: A Long, binary packed into an eight character string
        @rtype: L{str}
        """
        return self.timestamp_send_integer(timestamp.astimezone(self.utc).replace(tzinfo=None))


    def timestamptz_send_float(self, timestamp):
        """
        Convert a datetime.datetime into a binary packed string representation
        of a double-precision float representing seconds since 2000-01-01
        If the timestamp has timezone info, convert it to UTC.

        @param timestamp: A timestamp
        @type timestamp: L{datetime.datetime}

        @return: A Double, binary packed into an eight character string
        @rtype: L{str}
        """
        return self.timestamp_send_float(timestamp.astimezone(self.utc).replace(tzinfo=None))

    def timestamptz_recv_integer(self, data, offset, length):
        """
        Convert a binary-packed string representation of a 64-bit integer into
        a datetime.datetime in the UTC timezone.

        @param data: A binary-packed string representation of a 64-bit integer
        @type data: L{str}

        @param offset: Character offset into the data
        @type offset: L{int}

        @param length: Ignored
        @type length: L{int}

        @return: A datetime.datetime instance with UTC timezone
        @rtype: L{datetime.datetime}
        """
        micros = self.q_unpack(data, offset)[0]
        try:
            return self.epoch_tz + datetime.timedelta(microseconds=micros)
        except OverflowError as e:
            if micros == self.infinity_microseconds:
                return self.datetime_max_tz
            elif micros == self.minus_infinity_microseconds:
                return self.datetime_min_tz
            else:
                raise e

    def timestamptz_recv_float(self, data, offset, length):
        """
        Convert a binary-packed string representation of a double-precision
        float into a datetime.datetime in the UTC timezone.

        @param data: A binary-packed string representation of a double
        @type data: L{str}

        @param offset: Character offset into the data
        @type offset: L{int}

        @param length: Ignored
        @type length: L{int}

        @return: A datetime.datetime instance with UTC timezone
        @rtype: L{datetime.datetime}
        """
        return self.timestamp_recv_float(data, offset, length).replace(tzinfo=utc)

    def interval_send_integer(self, intrvl):
        """
        Convert an Interval object into three binary-packed Integers
        representing microseconds, days and months

        @param intrvl: A binary-packed string representation of a double
        @type intrvl: L{pg8000.interval.Interval}

        @return: A Long, binary packed into an eight character string
        @rtype: L{str}
        """
        microseconds = intrvl.microseconds
        try:
            microseconds += int(intrvl.seconds * 1e6)
        except AttributeError:
            pass

        try:
            months = intrvl.months
        except AttributeError:
            months = 0

        return self.qii_pack(microseconds, intrvl.days, months)


    def interval_send_float(self, intrvl):
        """
        Convert an Interval object into a binary-packed Double and two
        binary-packed integers representing seconds, days and months

        @param intrvl: A binary-packed string representation of a double
        @type intrvl: L{pg8000.interval.Interval}

        @return: Three floats, binary packed into an eight character string
        @rtype: L{str}
        """
        seconds = intrvl.microseconds / 1000.0 / 1000.0
        try:
            seconds += intrvl.seconds
        except AttributeError:
            pass

        try:
            months = intrvl.months
        except AttributeError:
            months = 0

        return self.dii_pack(seconds, intrvl.days, months)


    def interval_recv_integer(self, data, offset, length):
        """
        Convert a binary-packed string representation of three integers
        (representing microseconds, days and months) into an interval.Interval
        instance

        @param data: A binary-packed string representation of three integers
        @type data: L{str}

        @param offset: Character offset into the data
        @type offset: L{int}

        @param length: Ignored
        @type length: L{int}

        @return: A datetime.datetime instance
        @rtype: L{datetime.datetime}
        """
        microseconds, days, months = self.qii_unpack(data, offset)
        if months == 0:
            seconds, micros = divmod(microseconds, 1e6)
            return datetime.timedelta(days, seconds, micros)
        else:
            return interval.Interval(microseconds, days, months)


    def interval_recv_float(self, data, offset, length):
        """
        Convert a binary-packed string representation of a double and two
        integers (representing seconds, days and months) into an
        interval.Interval instance

        @param data: A binary-packed string representation of three integers
        @type data: L{str}

        @param offset: Character offset into the data
        @type offset: L{int}

        @param length: Ignored
        @type length: L{int}

        @return: A datetime.datetime instance
        @rtype: L{datetime.datetime}
        """
        seconds, days, months = self.dii_unpack(data, offset)
        if months == 0:
            secs, microseconds = divmod(seconds, 1e6)
            return datetime.timedelta(days, secs, microseconds)
        else:
            return interval.Interval(int(seconds * 1000 * 1000), days, months)


    def int8_recv(self, data, offset, length):
        """
        Convert a binary-packed string representation of a Long integer
        to an int

        @param data: A binary-packed string representation of an integer
        @type data: L{str}

        @param offset: Character offset into the data
        @type offset: L{int}

        @param length: Ignored
        @type length: L{int}
        
        @return: An integer
        @rtype: L{int}
        """
        return self.q_unpack(data, offset)[0]


    def int2_recv(self, data, offset, length):
        """
        Convert a binary-packed string representation of an integer
        to an int

        @param data: A binary-packed string representation of an integer
        @type data: L{str}

        @param offset: Character offset into the data
        @type offset: L{int}

        @param length: Ignored
        @type length: L{int}
        
        @return: An integer
        @rtype: L{int}
        """
        return self.h_unpack(data, offset)[0]


    def int4_recv(self, data, offset, length):
        """
        Convert a binary-packed string representation of an integer
        to an int

        @param data: A binary-packed string representation of an integer
        @type data: L{str}

        @param offset: Character offset into the data
        @type offset: L{int}

        @param length: Ignored
        @type length: L{int}
        
        @return: An integer
        @rtype: L{int}
        """
        return self.i_unpack(data, offset)[0]


    def float4_recv(self, data, offset, length):
        """
        Convert a binary-packed string representation of a float
        to a float

        @param data: A binary-packed string representation of a float
        @type data: L{str}

        @param offset: Character offset into the data
        @type offset: L{int}

        @param length: Ignored
        @type length: L{int}
        
        @return: A float
        @rtype: L{float}
        """
        return self.f_unpack(data, offset)[0]


    def float8_recv(self, data, offset, length):
        """
        Convert a binary-packed string representation of a double
        to a float

        @param data: A binary-packed string representation of a double
        @type data: L{str}

        @param offset: Character offset into the data
        @type offset: L{int}

        @param length: Ignored
        @type length: L{int}
        
        @return: A float
        @rtype: L{float}
        """
        return self.d_unpack(data, offset)[0]


    def int_in(self, data, offset, length):
        """
        Extract an int from a string

        @param data: A string including an integer value
        @type data: L{str}

        @param offset: Character offset into the data
        @type offset: L{int}

        @param length: Number of characters to extract
        @type length: L{int}
        
        @return: An int
        @rtype: L{int}
        """
        return int(data[offset: offset + length])


    def text_out(self, data):
        """
        Return data encoded in current client encoding

        @param data: A string or unicode value
        @type data: L{str}

        @return: Bytes
        @rtype: L{bytes}
        """
        return data.encode(self.client_encoding)


    def time_out(self, data):
        """
        Return datetime data as isoformat encoded in current client encoding

        @param data: A datetime instance
        @type data: L{datetime.datetime}

        @return: Bytes
        @rtype: L{bytes}
        """
        return data.isoformat().encode(self.client_encoding)


    def date_out(self, data):
        """
        Return date data as isoformat encoded in current client encoding

        @param data: A datetime.date instance
        @type data: L{datetime.date}

        @return: Bytes
        @rtype: L{bytes}
        """
        if data == datetime.date.max:
            return 'infinity'.encode(self.client_encoding)
        elif data == datetime.date.min:
            return '-infinity'.encode(self.client_encoding)
        else:
            return data.isoformat().encode(self.client_encoding)


    def unknown_out(self, data):
        """
        Return unknown data cast as string and encoded in current client
        encoding

        @param data: Some data
        @type data: C{unknown}

        @return: Bytes
        @rtype: L{bytes}
        """
        return str(data).encode(self.client_encoding)


    def num_array_in(self, data, offset, length):
        """
        Extract a list of Decimals from a binary-encoded string

        @param data: A bytestring
        @type data: L{bytes}

        @param offset: Character offset into the data
        @type offset: L{int}

        @param length: Number of characters to extract
        @type length: L{int}
        
        @return: A list of Decimals
        @rtype: L{list}
        """
        print("WARNING: POTENTIALLY DANGEROUS - ?")
        arr = []
        prev_c = None
        data = data[offset:offset+length].decode(self.client_encoding)
        data = data.translate(self.translation_table)
        data = data.replace(six.u('NULL'), six.u('None'))
        for c in data:
            if c not in ('[', ']', ',', 'N') and prev_c in ('[', ','):
                arr.extend("Decimal('")
            elif c in (']', ',') and prev_c not in ('[', ']', ',', 'e'):
                arr.extend("')")

            arr.append(c)
            prev_c = c
        return eval(''.join(arr), self.glbls)


    def array_recv(self, data, offset, length):
        """
        Extract a list of items from a binary-encoded string

        @param data: A bytestring
        @type data: L{bytes}

        @param offset: Character offset into the data
        @type offset: L{int}

        @param length: Number of characters to extract
        @type length: L{int}
        
        @return: A list of objects
        @rtype: L{list}
        """
        final_idx = offset + length
        dim, hasnull, typeoid = self.iii_unpack(data, offset)
        offset += 12

        # get type conversion method for typeoid
        conversion = self.pg_types[typeoid][1]

        # Read dimension info
        dim_lengths = []
        for i in range(dim):
            dim_lengths.append(self.ii_unpack(data, offset)[0])
            offset += 8

        # Read all array values
        values = []
        while offset < final_idx:
            element_len, = self.i_unpack(data, offset)
            offset += 4
            if element_len == -1:
                values.append(None)
            else:
                values.append(conversion(data, offset, element_len))
                offset += element_len

        # at this point, {{1,2,3},{4,5,6}}::int[][] looks like
        # [1,2,3,4,5,6]. go through the dimensions and fix up the array
        # contents to match expected dimensions
        for length in reversed(dim_lengths[1:]):
            values = list(map(list, zip(*[iter(values)] * length)))
        return values


    def bytea_send(self, data):
        """
        Return byte data, unmodified
        """
        return data


    def _create_bytea_recv_func(self):
        if six.PY2:
            def bytea_recv(data, offset=0, length=0):
                return types.Bytea(data[offset:offset + length])
        else:
            def bytea_recv(data, offset=0, length=0):
                return data[offset:offset + length]

        bytea_recv.__doc__ = """
            Cast received data from bytes to bytes

            @param data: Bytes
            @type data: L{str}

            @param offset: Character offset into the data
            @type offset: L{int}

            @param length: Ignored
            @type length: L{int}
            
            @return: Bytes
            @rtype: L{str}
            """
        setattr(self, "bytea_recv", bytea_recv)

    def _create_text_recv_func(self):
        if six.PY2:
            def text_recv(data, offset, length):
                return unicode(  # noqa
                    data[offset: offset + length], self.client_encoding)

            def bool_recv(data, offset, length):
                return data[offset] == "\x01"

            def json_in(data, offset, length):
                return json.loads(unicode(  # noqa
                    data[offset: offset + length], self.client_encoding))

        else:
            def text_recv(data, offset, length):
                return str(
                    data[offset: offset + length], self.client_encoding)

            def bool_recv(data, offset, length):
                return data[offset] == 1

            def json_in(data, offset, length):
                return json.loads(
                    str(data[offset: offset + length], self.client_encoding))

        text_recv.__doc__ = """
            Decode received data using client encoding and return resulting
            Unicode object

            @param data: Bytes
            @type data: L{str}

            @param offset: Character offset into the data
            @type offset: L{int}

            @param length: Number of characters to extract
            @type length: L{int}
            
            @return: Unicode object
            @rtype: L{unicode}
        """

        text_recv.__doc__ = """
            Interpret the single character at 'offset' in a bytestring as a
            boolean object

            @param data: Bytes
            @type data: L{str}

            @param offset: Character offset into the data
            @type offset: L{int}

            @param length: Ignored
            @type length: L{int}
            
            @return: Boolean object
            @rtype: L{bool}
        """

        json_in.__doc__ = """
            Decode received data using client encoding and parse resulting json
            data into a Python object

            @param data: Bytes
            @type data: L{str}

            @param offset: Character offset into the data
            @type offset: L{int}

            @param length: Number of bytes of data to extract
            @type length: L{int}
            
            @return: Python object
            @rtype: L{object}
            """

        setattr(self, "text_recv", text_recv)
        setattr(self, "bool_recv", bool_recv)
        setattr(self, "json_in",   json_in)


    def time_in(self, data, offset, length):
        """
        Extract a datetime from a binary-encoded string

        @param data: A bytestring
        @type data: L{bytes}

        @param offset: Character offset into the data
        @type offset: L{int}

        @param length: Number of characters to extract
        @type length: L{int}
        
        @return: A datetime object
        @rtype: L{datetime.datetime}
        """
        hour = int(data[offset:offset + 2])
        minute = int(data[offset + 3:offset + 5])
        sec = decimal.Decimal(
            data[offset + 6:offset + length].decode(self.client_encoding))
        return datetime.time(
            hour, minute, int(sec), int((sec - int(sec)) * 1000000))


    def date_in(self, data, offset, length):
        """
        Extract a date from a binary-encoded string

        @param data: A bytestring
        @type data: L{bytes}

        @param offset: Character offset into the data
        @type offset: L{int}

        @param length: Number of characters to extract
        @type length: L{int}
        
        @return: A date object
        @rtype: L{datetime.date}
        """
        year_str = data[offset:offset + 4].decode(self.client_encoding)
        if year_str == 'infi':
            return datetime.date.max
        elif year_str == '-inf':
            return datetime.date.min
        else:
            return datetime.date(
                int(year_str), int(data[offset + 5:offset + 7]),
                int(data[offset + 8:offset + 10]))


    def numeric_in(self, data, offset, length):
        """
        Extract a Decimal from a binary-encoded string

        @param data: A bytestring
        @type data: L{bytes}

        @param offset: Character offset into the data
        @type offset: L{int}

        @param length: Number of characters to extract
        @type length: L{int}
        
        @return: A Decimal object
        @rtype: L{decimal.Decimal}
        """
        return decimal.Decimal(
            data[offset: offset + length].decode(self.client_encoding))


    def numeric_out(self, number):
        """
        Convert a number to a string and encode it using the client encoding

        @param number: An integer, float or Decimal
        @type number: C{numeric}

        @return: Bytes
        @rtype: L{bytes}
        """
        return str(number).encode(self.client_encoding)


    def vector_in(self, data, offset, length):
        """
        Extract a list from a binary-encoded string

        @param data: A bytestring
        @type data: L{bytes}

        @param offset: Character offset into the data
        @type offset: L{int}

        @param length: Number of characters to extract
        @type length: L{int}
        
        @return: A list
        @rtype: L{list}
        """
        return eval('[' + data[offset:offset+length].decode(
            self.client_encoding).replace(' ', ',') + ']')


    def uuid_send(self, data):
        """
        Given a UUID, return a bytes representation of it

        @param data: A UUID
        @type data: L{uuid.UUID}

        @return: Bytes
        @rtype: L{bytes}
        """
        return data.bytes


    def uuid_recv(self, data, offset, length):
        """
        Extract a UUID from a binary-encoded string

        @param data: A bytestring
        @type data: L{bytes}

        @param offset: Character offset into the data
        @type offset: L{int}

        @param length: Number of characters to extract
        @type length: L{int}
        
        @return: A UUID
        @rtype: L{uuid.UUID}
        """
        return uuid.UUID(bytes=data[offset:offset+length])


    def inet_out(self, data):
        """
        Encode an ip address as bytes
        """
        return str(data).encode(self.client_encoding)


    def inet_in(self, data, offset, length):
        """
        Extract an ip address from a binary-encoded string

        @param data: A bytestring
        @type data: L{bytes}

        @param offset: Character offset into the data
        @type offset: L{int}

        @param length: Number of characters to extract
        @type length: L{int}
        
        @return: An ip_network or ip_address instance
        @rtype: C{object}
        """
        inet_str = data[offset: offset + length].decode(self.client_encoding)
        if '/' in inet_str:
            return ipaddress.ip_network(inet_str, False)
        else:
            return ipaddress.ip_address(inet_str)


    def bool_send(self, value):
        return self.true if value else false


    def null_send(self):
        return self.null

    def get_translator_for_complex_type(self, typ, value):
        if typ == datetime.datetime:
            return self.inspect_datetime(value)
        if typ in (list, tuple):
            return self.inspect_array(value)

        raise NotSupportedError("type %s not mapped to pg type" % (typ,))

    def get_translators(self, values):
        translators = []
        for value in values:
            typ = type(value)
            try:
                translators.append(self.py_types[typ])
            except KeyError:
                translator = self.get_translator_for_complex_type(typ, value)
                translators.append(translator)

        return tuple(translators)

    def inspect_datetime(self, value):
        if value.tzinfo is None:
            return self.py_types[1114]  # timestamp
        else:
            return self.py_types[1184]  # send as timestamptz

    def inspect_array(self, value):

        flattened = self.flatten_array(value)

        # Check if array has any values.  If not, we can't determine the proper
        # array oid.
        if not flattened:
            raise errors.ArrayContentEmptyError("array has no values")

        # supported array output
        typ = type(flattened[0])
        cons_ = constants.Constants

        if issubclass(typ, six.integer_types):
            # special int array support -- send as smallest possible array type
            typ = six.integer_types
            int2_ok, int4_ok, int8_ok = True, True, True
            for v in flattened:
                if v is None:
                    continue
                if cons_.MIN_INT2 < v < cons_.MAX_INT2:
                    continue
                int2_ok = False
                if cons_.MIN_INT4 < v < cons_.MAX_INT4:
                    continue
                int4_ok = False
                if cons_.MIN_INT8 < v < cons_.MAX_INT8:
                    continue
                int8_ok = False
            if int2_ok:
                array_oid = 1005  # INT2[]
                oid, fc, send_func = (21, self.FC_BINARY, h_pack)
            elif int4_ok:
                array_oid = 1007  # INT4[]
                oid, fc, send_func = (23, self.FC_BINARY, i_pack)
            elif int8_ok:
                array_oid = 1016  # INT8[]
                oid, fc, send_func = (20, self.FC_BINARY, q_pack)
            else:
                raise errors.ArrayContentNotSupportedError(
                    "numeric not supported as array contents")
        else:
            try:
                oid, fc, send_func = self.get_translators((first_element,))[0]

                # If unknown, assume it's a string array
                if oid == 705:
                    oid = 25
                    # Use binary ARRAY format to avoid having to properly
                    # escape text in the array literals
                    fc = self.FC_BINARY
                array_oid = cons_.PG_ARRAY_TYPES[oid]
            except KeyError:
                raise errors.ArrayContentNotSupportedError(
                    "oid " + str(oid) + " not supported as array contents")
            except NotSupportedError:
                raise errors.ArrayContentNotSupportedError(
                    "type " + str(typ) + " not supported as array contents")

        if fc == self.FC_BINARY:
            def send_array(arr):
                # check for homogenous array
                for a, i, v in walk_array(arr):
                    if not isinstance(v, (typ, type(None))):
                        raise errors.ArrayContentNotHomogenousError(
                            "not all array elements are of type " + str(typ))

                # check that all array dimensions are consistent
                array_check_dimensions(arr)

                has_null = array_has_null(arr)
                dim_lengths = array_dim_lengths(arr)
                data = bytearray(iii_pack(len(dim_lengths), has_null, oid))
                for i in dim_lengths:
                    data.extend(ii_pack(i, 1))
                for v in self.array_flatten(arr):
                    if v is None:
                        data += i_pack(-1)
                    else:
                        inner_data = send_func(v)
                        data += i_pack(len(inner_data))
                        data += inner_data
                return data
        else:
            def send_array(arr):
                for a, i, v in walk_array(arr):
                    if not isinstance(v, (typ, type(None))):
                        raise errors.ArrayContentNotHomogenousError(
                            "not all array elements are of type " + str(typ))
                array_check_dimensions(arr)
                ar = deepcopy(arr)
                for a, i, v in walk_array(ar):
                    if v is None:
                        a[i] = 'NULL'
                    else:
                        a[i] = send_func(v).decode('ascii')

                return u(str(ar)).translate(arr_trans).encode('ascii')
        return (array_oid, fc, send_array)

    def array_flatten(self, arr):
        for v in arr:
            if isinstance(v, list):
                for v2 in self.array_flatten(v):
                    yield v2
            else:
                yield v

    def _create_pack_funcs(self, fmt):
        struc = struct.Struct('!' + fmt)

        def pack(*args):
            return struc.pack(*args)
        pack.__doc__ = "struct.Struct('!%s').pack" % (fmt,)
        pack.__name__ = fmt + "_pack"
        setattr(self, pack.__name__, pack)

        def unpack(*args):
            return struc.unpack_from(*args)
        unpack.__doc__ = "struct.Struct('!%s').unpack_from" % (fmt,)
        unpack.__name__ = fmt + "_unpack"
        setattr(self, unpack.__name__, unpack)

    def _create_pack_shorts(self):
        self.pack_shorts = {}
        self.unpack_shorts = {}

        for index in range(1, self.max_pack_shorts_count):
            fmt = index * "h"
            self._create_pack_funcs(fmt)
            self.pack_shorts[index]   = getattr(self, fmt + "_pack")
            self.unpack_shorts[index] = getattr(self, fmt + "_unpack")

    def _create_pack_shorts_for_index(self, index):
        fmt = int(index) * "h"
        self._create_pack_funcs(fmt)
        self.pack_shorts[index]   = getattr(self, fmt + "_pack")
        self.unpack_shorts[index] = getattr(self, fmt + "_unpack")
