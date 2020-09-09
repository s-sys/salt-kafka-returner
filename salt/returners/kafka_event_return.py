# -*- coding: utf-8 -*-
'''
Return events to a Kafka topic

:maintainer: Felipe Lima Morais (felipe.morais@ssys.com.br)
:maturity: 20200214
:depends: python-kafka-python
:platform: all

Configuration parameters:

.. code-block:: yaml

    hostnames: localhost:9092
        # List of Kafka servers to connect to.
    topic: salt_return
        # Topic used in the Kafka server that sends the messages.
    returner_persist_conn: True
        # If `returner_persist_conn` is true, the `KafkaProducer` will be
        # instanced and saved in `__context__`. Otherwise, the `KafkaProducer`
        # will be instanced for each event.
    force_send: False
    retries: 5
        # Define how many retries should be attempted before dropping the
        # messages.

To enable this returner, install `kafka-python` and enable the following
settings in the minion config:

.. code-block:: yaml

    kafka_event_return:
      hostnames:
        - "server1"
        - "server2"
        - "server3"
      topic: salt_returner
      returner_persist_conn: True
      force_send: False

To use the Kafka event returner, the following configuration must be set in the
master config:

.. code-block:: yaml

    event_return: kafka_event_return

To use the Kafka returner, append '--return kafka_event_return' to the
SaltStack command:
    salt '*' test.ping --return kafka_event_return
'''

from __future__ import absolute_import, print_function, unicode_literals

import logging

import salt.returners
import salt.utils.json

try:
    from kafka import KafkaProducer
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False

__virtualname__ = 'kafka_event_return'

log = logging.getLogger(__name__)


def __virtual__():
    '''
    Check if `python-kafka` is installed
    '''
    if not HAS_KAFKA:
        log.error('kafka_event_return: \'kafka-python\' is not installed')
        return False
    return __virtualname__


def _get_options(ret=None):
    '''
    Get the returner options from SaltStack
    '''
    defaults = {
        'hostnames': ['localhost:9092'],
        'topic': 'salt_return',
        'returner_persist_conn': True,
        'force_send': False,
        'retries': 5
    }
    attrs = {
        'hostnames': 'hostnames',
        'topic': 'topic',
        'returner_persist_conn': 'returner_persist_conn',
        'force_send': 'force_send',
        'retries': 'retries'
    }
    options = salt.returners.get_returner_options(
        __virtualname__, ret, attrs,
        __salt__=__salt__,  # pylint: disable=undefined-variable
        __opts__=__opts__,
        defaults=defaults
    )
    return options


def _get_conn(ret=None):
    '''
    Return a kafka connection
    '''
    # Get returner options from SaltStack
    options = _get_options(ret)
    if options['returner_persist_conn']:
        log.debug('kafka_event_return: Reading returner options from SaltStack.')
        try:
            return __context__['kafka_returner_conn']  # pylint: disable=undefined-variable
        except Exception as exc:  # pylint: disable=broad-except
            log.debug('kafka_event_return: Failed to read returner options. Exception: %s.', str(exc))

    hostnames = options['hostnames']
    try:
        log.debug('kafka_event_return: connecting to (%s)', hostnames)
        __context__['kafka_returner_conn'] = KafkaProducer(  # pylint: disable=undefined-variable
            bootstrap_servers=hostnames,
            value_serializer=lambda v: salt.utils.json.dumps(v).encode('utf-8'),
            retries=options['retries'],
        )
    except Exception as exc:  # pylint: disable=broad-except
        log.error('kafka_event_return: Error connecting to \'%s\'. Exception: %s.', hostnames, exc)

    return __context__['kafka_returner_conn']  # pylint: disable=undefined-variable


def returner(ret):
    '''
    Return information to a Kafka server
    '''
    # Get returner options from SaltStack
    options = _get_options(ret)
    try:
        log.debug('kafka_event_return: Sending returner data to the Kafka server.')
        if options['topic']:
            topic = options['topic']
            conn = _get_conn(ret)
            conn.send(topic, ret)
            if options['force_send']:
                conn.flush()
        else:
            log.error('kafka_event_return: Topic \'%s\' not found.', options['topic'])
    except Exception as exc:  # pylint: disable=broad-except
        log.error('kafka_event_return: Error sending data to the Kafka server. Exception: %s.', exc)


def event_return(events):
    '''
    Return events to Kafka server

    Requires the `event_return` config to be set in the master config.
    '''
    # Get returner options from SaltStack
    options = _get_options()
    log.debug('kafka_event_return: Processing message. Events: \'%s\'', events)
    try:
        data = []
        for event in events:
            data.append({
                'tag': event.get('tag', ''),
                'data': event.get('data', ''),
            })
    except Exception as exc:  # pylint: disable=broad-except
        log.error('kafka_event_return: Error processing message. Exception: %s.', exc)

    log.debug('kafka_event_return: Sending message to the Kafka server.')
    try:
        if options['topic']:
            topic = options['topic']
            conn = _get_conn()
            conn.send(topic, data)
            log.debug('kafka_event_return: Sending message to the Kafka server. Topic: \'%s\', Message: \'%s\'.',
                      topic, data)
            if options['force_send']:
                conn.flush()
        else:
            log.error('kafka_event_return: Unable to find topic \'%s\'.', options['topic'])
    except Exception as exc:  # pylint: disable=broad-except
        log.error('kafka_event_return: Error sending message to the Kafka server. Exception: %s.', exc)
