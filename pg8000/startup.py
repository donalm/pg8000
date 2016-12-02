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
from pg8000 import translate
from pg8000 import constants
from pg8000 import errors


class StartupMessage(object):

    @classmethod
    def construct(cls, database, user,
                       protocol_version=None,
                       client_encoding="utf-8",
                       application_name=None,
                       **kwargs):

        if application_name is None:
            application_name = constants.Constants.APPLICATION_NAME

        return cls._construct(database,
                              user,
                              protocol_version,
                              client_encoding,
                              application_name,
                              **kwargs)

    @classmethod
    def _construct(cls, database, user,
                       protocol_version=None,
                       client_encoding="utf-8",
                       application_name=None,
                       **kwargs):
        cls.database = database
        cls.user = user
        cls.protocol_version = protocol_version or \
                               constants.Constants.DEFAULT_PROTOCOL_VERSION

        translator = translate.PostgresTranslate(client_encoding=client_encoding)
        cls.translator = translator

        # Int32 - Message length, including self.
        # Int32(protocol_version) - Protocol version number.
        # Any number of key/value pairs, terminated by a zero byte:
        #   String - A parameter name (user, database, or options)
        #   String - Parameter value
        message = translator.i_pack(cls.protocol_version)
        message += cls.make_argument("user", cls.user)

        if database is not None:
            message += cls.make_argument("database", database)

        if application_name:
            message += cls.make_argument("application_name", application_name)

        for key, value in kwargs.items():
            message += cls.make_argument(key, value)

        # End of message
        message += translator.null_byte

        message_length = len(message)

        # Add four bytes to store the encoded message length value itself
        message_length += 4

        # The message is the l
        return bytes(translator.i_pack(message_length) + message)


    @classmethod
    def make_argument(cls, key, value):

        if isinstance(key, six.text_type):
            key = key.encode('utf8')

        if not isinstance(key, six.binary_type):
            raise errors.ProgrammingError(
                "Expected bytes, got '%s' for startup "\
                "message key '%s'" % (type(key), key,)
            )

        if isinstance(value, six.text_type):
            value = value.encode('utf8')

        if not isinstance(value, six.binary_type):
            raise errors.ProgrammingError(
                "Expected bytes, got '%s' for startup "\
                "message value '%s'" % (type(value), value,)
            )

        return key   + cls.translator.null_byte + \
               value + cls.translator.null_byte


def message(database, user, application_name=None):
    return StartupMessage._construct(database, user, application_name=application_name)
