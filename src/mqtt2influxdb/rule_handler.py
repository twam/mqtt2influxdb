import asyncio
import json
import logging
import re
from functools import lru_cache

from aiomqtt import Topic, Wildcard

from . import topic

class RuleHandler:
    config = {}

    _stop_event = None

    def __init__(self, config, mqtt, influxdb):
        self._mqtt = mqtt
        self._influxdb = influxdb
        self._stop_event = asyncio.Event()

        self._parseConfiguration(config)

    async def subscribe_topics(self):
        for normalized_topic in self._normalizedTopics:
            await self._mqtt.subscribe(normalized_topic)

    def finish(self):
        logging.info("Finishing topic handler ...")
        self._stop_event.set()

    @lru_cache(maxsize=128)
    def _getMatchingRules(self, topic):
        topic_obj = Topic(topic)
        return [rule for rule in self._rules if topic_obj.matches(rule["wildcard"])]

    async def run(self):
        while not self._stop_event.is_set():
            try:
                item = await self._mqtt.getQueue().get()
                if item is None:
                    self._mqtt.getQueue().task_done()
                    continue
                await self._process_item(item)
                self._mqtt.getQueue().task_done()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.error(f'Error while sending from mqtt to db: {type(e).__name__}: {e}')

    async def _process_item(self, item):
        msg, received_at = item

        # Decode once; the payload is used by logging, the parser and
        # the simple field conversion below.
        payload_str = msg.payload.decode('utf-8', errors='replace')

        logging.debug("MQTT message: topic=%s payload=%s qos=%s retain=%s", msg.topic, payload_str, msg.qos, msg.retain)
        handledCounter = 0

        for rule in self._getMatchingRules(msg.topic):
                    topicObject = rule['topicObject']
                    # Handle message for all registered topics for this normalized topic

                    retain = rule['retain'] if ('retain' in rule) else False
                    if msg.retain and not retain:
                        logging.debug("Ignore retained message for topic '%s'", msg.topic)
                        continue

                    matches = topicObject.parse(msg.topic)

                    if matches is not None:
                        db_inserts = []

                        # primary insert
                        db_insert = {
                            'fields': {},
                            'tags': {}
                        }

                        if ('payload' in rule):
                            name = rule['payload'].get('name', 'payload')

                            if 'compiled_parser' in rule['payload']:
                                try:
                                    locals_ = {'payload': json.loads(payload_str)}
                                except json.decoder.JSONDecodeError:
                                    locals_ = {'payload': payload_str}
                                locals_['tokens'] = {tokenName: tokenValue for tokenName, tokenValue in matches.items()}

                                exec(rule['payload']['compiled_parser'], {}, locals_)

                                for key in ['fields', 'tags', 'measurement']:
                                    if key in locals_:
                                        db_insert[key] = locals_[key]

                                if 'inserts' in locals_:
                                    if isinstance(locals_['inserts'], list):
                                        db_inserts += locals_['inserts']
                                    else:
                                        raise TypeError("inserts must be of type list")

                            if rule['payload'].get('field', False):
                                db_insert['fields'][name] = self._convertToType(payload_str, rule['payload'].get('type', None), rule['payload'].get('json', None))

                            # if ('tag' in rule['payload']) and (rule['payload']['tag'] == True):
                            #     db_insert['fields'][name] = self._convertToType(msg.payload.decode("UTF-8"), 'string')

                        if ('fields' in rule) and (rule['fields'] is not None):
                            for fieldName, fieldValue in rule['fields'].items():
                                db_insert['fields'][fieldName] = fieldValue

                        if ('tags' in rule) and (rule['tags'] is not None):
                            for tagName, tagValue in rule['tags'].items():
                                db_insert['tags'][tagName] = tagValue

                        if rule.get('measurement', None) is not None:
                            db_insert['measurement'] = self._convertToType(rule['measurement'], 'string')

                        for tokenName, tokenValue in matches.items():
                            #print(rule.get('tokens', None))

                            if tokenName in rule.get('tokens', []):
                                tokenConfig = rule['tokens'][tokenName]
                                field_name = tokenConfig.get('field_name', tokenName)
                                tag_name = tokenConfig.get('tag_name', tokenName)

                                if tokenConfig.get('field', False):
                                    db_insert['fields'].update({field_name: str(tokenValue)})

                                if tokenConfig.get('field_map', {}) != {}:
                                    db_insert['fields'].update({field_name: str(tokenConfig['field_map'][tokenValue])})

                                if tokenConfig.get('tag', False):
                                    db_insert['tags'].update({tag_name: str(tokenValue)})

                                if tokenConfig.get('tag_map', {}) != {}:
                                    db_insert['tags'].update({tag_name: str(tokenConfig['tag_map'][tokenValue])})

                                if tokenConfig.get('measurement', False):
                                    db_insert['measurement'] = tokenValue

                                if tokenConfig.get('measurement_map', {}) != {}:
                                    db_insert['measurement'] = str(tokenConfig['measurement_map'][tokenValue])


                        # Check db_insert
                        if (len(db_insert['fields']) > 0) and (len(db_insert['tags']) > 0):
                            if 'measurement' not in db_insert:
                                logging.error(f'No measurement for rule {topicObject.topic}: {db_insert}')

                            db_inserts.append(db_insert)

                        for insert in db_inserts:
                            if 'time' not in insert:
                                insert['time'] = received_at

                        if handledCounter > 0:
                            logging.warning("Message for topic '%s' already handled %s times", msg.topic, handledCounter)

                        handledCounter += 1

                        logging.debug('Send to db: %s', db_insert)
                        try:
                            if not rule.get('disable_write', False):
                                self._influxdb.write(db_inserts)
                            else:
                                logging.info("Not writing: %s", db_inserts)
                        except Exception as e:
                            logging.error(f'Could not insert into db: {e}')

    def _parseConfiguration(self, config):
        self._topicObjects = []
        self._normalizedTopics = {}

        # Load Rules
        self._rules = config.get("rules", None)

        if self._rules is None:
            raise ValueError("No configuration section for Rules")

        for index, rule in enumerate(self._rules):
            if 'topic' not in rule:
                logging.error("No 'topic' for rule #%u" % (index))
                continue

            # Create topic object
            topicObject = topic.Topic(rule['topic'])
            rule['topicObject'] = topicObject
            rule['wildcard'] = Wildcard(topicObject.normalized)

            # Add topic to list of normalized Topics
            if topicObject.normalized not in self._normalizedTopics:
                self._normalizedTopics[topicObject.normalized] = []

            self._normalizedTopics[topicObject.normalized].append(rule)

            # Add tokens to topic
            if ('tokens' in rule) and isinstance(rule['tokens'], dict):
                for tokenName, tokenData in rule['tokens'].items():
                    if 'rule' in tokenData:
                        topicObject.addTokenRule(tokenName, tokenData['rule'])

            # Pre-compile parser
            if 'payload' in rule and 'parser' in rule['payload']:
                rule['payload']['compiled_parser'] = compile(rule['payload']['parser'], '<string>', 'exec')

    def _convertToType(self, value, type_ = None, json_ = None):
        if type_ is None:
            if re.match(r"^\d+?\.?\d+?", value):
                return self._convertToType(value, 'float')
            elif re.match(r"^(true|True|TRUE|false|False|FALSE)$", value):
                return self._convertToType(value, 'bool')
            else:
                return self._convertType(value, 'string')
        elif type_ == 'int':
            return int(value)
        elif type_ == 'float':
            return float(value)
        elif type_ == 'bool':
            return bool(value)
        elif type_ == 'string':
            return str(value)
        elif type_ == 'json':
            json_splitted = json_.split(',')
            ret = json.loads(value)
            for i in json_splitted:
                ret = ret[i]
            return str(ret)
        else:
            raise Exception("Invalid type '%s'" % type_)
