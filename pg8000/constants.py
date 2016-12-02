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

import os
import sys
import six
import socket

class Constants(object):

    # Field constants
    FC_TEXT = 0
    FC_BINARY = 1

    # Default protocol version
    VERSION_MAJOR    = 3
    VERSION_MINOR    = 0
    DEFAULT_PROTOCOL_VERSION = (VERSION_MAJOR << 16) | VERSION_MINOR

    # Message codes
    NOTICE_RESPONSE = six.b("N")
    AUTHENTICATION_REQUEST = six.b("R")
    PARAMETER_STATUS = six.b("S")
    BACKEND_KEY_DATA = six.b("K")
    READY_FOR_QUERY = six.b("Z")
    ROW_DESCRIPTION = six.b("T")
    ERROR_RESPONSE = six.b("E")
    DATA_ROW = six.b("D")
    COMMAND_COMPLETE = six.b("C")
    PARSE_COMPLETE = six.b("1")
    BIND_COMPLETE = six.b("2")
    CLOSE_COMPLETE = six.b("3")
    PORTAL_SUSPENDED = six.b("s")
    NO_DATA = six.b("n")
    PARAMETER_DESCRIPTION = six.b("t")
    NOTIFICATION_RESPONSE = six.b("A")
    COPY_DONE = six.b("c")
    COPY_DATA = six.b("d")
    COPY_IN_RESPONSE = six.b("G")
    COPY_OUT_RESPONSE = six.b("H")
    EMPTY_QUERY_RESPONSE = six.b("I")

    BIND = six.b("B")
    PARSE = six.b("P")
    EXECUTE = six.b("E")
    FLUSH = six.b("H")
    SYNC = six.b("S")
    PASSWORD = six.b("p")
    DESCRIBE = six.b("D")
    TERMINATE = six.b("X")
    CLOSE = six.b("C")

    # DESCRIBE constants
    STATEMENT = six.b("S")
    PORTAL = six.b("P")

    # ErrorResponse codes
    RESPONSE_SEVERITY = six.b("S")  # always present
    RESPONSE_CODE = six.b("C")  # always present
    RESPONSE_MSG = six.b("M")  # always present
    RESPONSE_DETAIL = six.b("D")
    RESPONSE_HINT = six.b("H")
    RESPONSE_POSITION = six.b("P")
    RESPONSE__POSITION = six.b("p")
    RESPONSE__QUERY = six.b("q")
    RESPONSE_WHERE = six.b("W")
    RESPONSE_FILE = six.b("F")
    RESPONSE_LINE = six.b("L")
    RESPONSE_ROUTINE = six.b("R")

    IDLE = six.b("I")
    IDLE_IN_TRANSACTION = six.b("T")
    IDLE_IN_FAILED_TRANSACTION = six.b("E")

    try:
        HOSTNAME = socket.gethostname()
    except Exception as e:
        HOSTNAME = "UNKNOWNHOST"

    try:
        PID = str(os.getpid())
    except Exception as e:
        PID = "UNKNOWNPID"

    try:
        EXECUTABLE = os.path.basename(sys.executable)
    except Exception as e:
        EXECUTABLE = "UNKNOWNEXECUTABLE"

    APPLICATION_NAME = "%s_%s_%s" % (HOSTNAME, PID, EXECUTABLE,)

    MIN_INT2 = -2 ** 15
    MAX_INT2 =  2 ** 15
    MIN_INT4 = -2 ** 31
    MAX_INT4 =  2 ** 31
    MIN_INT8 = -2 ** 63
    MAX_INT8 =  2 ** 63

    PG_ARRAY_TYPES = {
        16: 1000,
        25: 1009,    # TEXT[]
        701: 1022,
        1700: 1231,  # NUMERIC[]
    }
