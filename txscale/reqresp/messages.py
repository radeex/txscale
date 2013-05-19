"""
Common code for dealing with low-level messages.

Any given scalable transport is not required to use these functions to generate and parse
messages, but they're useful if the low-level protocol is binary-safe.
"""

from uuid import uuid4
from collections import namedtuple


Request = namedtuple("request", ["message_id", "response_channel", "data"])
Response = namedtuple("response", ["message_id", "data"])


def splitRequest(message, delimeter=":"):
    message_id = message[:16]
    response_channel, data = message[16:].split(delimeter, 1)
    return Request(message_id, response_channel, data)


def generateRequest(response_channel, data, delimeter=":"):
    message_id = uuid4().bytes
    return (message_id, message_id + response_channel + delimeter + data)


def generateResponse(message_id, data):
    return message_id + data


def splitResponse(data):
    return Response(data[:16], data[16:])
