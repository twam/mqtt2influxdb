import paho.mqtt.client as mqttClient
import logging
import threading
import re
import json

from . import topic

class RuleHandler:
    config = {}

    _threads = []
    _stopEvent = threading.Event()

    def __init__(self, config, mqtt, influxdb):
        self._mqtt = mqtt
        self._influxdb = influxdb

        queueHandlerThread = threading.Thread(target=self._queueHandler, name="ruleHandlerQueueHandler")
        queueHandlerThread.daemon = True
        queueHandlerThread.start()
        self._threads.append(queueHandlerThread)

        self._parseConfiguration(config)
        self._subcribeMqttTopics()

    def finish(self):
        logging.info("Finishing topic handler ...")
        self._stopEvent.set()

    def _queueHandler(self):
        logging.info("Starting Queue handler ...")
        while not self._stopEvent.is_set():
            try:
                msg = self._mqtt.getQueue().get()
                if msg is None:
                    self._mqtt.getQueue().task_done()
                    continue

                logging.debug("MQTT message: topic="+msg.topic+" payload="+msg.payload.decode('utf-8')+" qos="+str(msg.qos)+" retain="+str(msg.retain))
                handledCounter = 0

                for normalizedTopic, rules in self._normalizedTopics.items():
                    if mqttClient.topic_matches_sub(normalizedTopic, msg.topic):
                        # Message matches normalized topic
                        for rule in rules:
                            topicObject = rule['topicObject']
                            # Handle message for all registered topics for this normalized topic

                            retain = rule['retain'] if ('retain' in rule) else False
                            if msg.retain and not retain:
                                logging.debug(f"Ignore retained message for topic '{msg.topic}'")
                                continue

                            matches = topicObject.parse(msg.topic)

                            if (matches != None):
                                db_inserts = [{
                                    'fields': {},
                                    'tags': {}
                                }]
                                # primary insert
                                db_insert = db_inserts[0]

                                if ('payload' in rule):
                                    name = rule['payload'].get('name', 'payload')

                                    if 'parser' in rule['payload']:
                                        try:
                                            locals_ = {'payload': json.loads(msg.payload.decode("UTF-8"))}
                                        except json.decoder.JSONDecodeError:
                                            locals_ = {'payload': msg.payload.decode("UTF-8")}
                                        exec(rule['payload']['parser'], {}, locals_)

                                        if 'fields' in locals_:
                                            db_insert['fields'] = locals_['fields']

                                    if rule['payload'].get('field', False):
                                        db_insert['fields'][name] = self._convertToType(msg.payload.decode("UTF-8"), rule['payload'].get('type', None), rule['payload'].get('json', None))

                                    # if ('tag' in rule['payload']) and (rule['payload']['tag'] == True):
                                    #     db_insert['fields'][name] = self._convertToType(msg.payload.decode("UTF-8"), 'string')

                                if ('fields' in rule) and (rule['fields'] is not None):
                                    for fieldName, fieldValue in rule['fields'].items():
                                        db_insert['tag'][fieldName] = fieldValue

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

                                # Check db_insert
                                if 'measurement' not in db_insert:
                                    logging.error(f'No measurement for rule {topicObject.topic}')

                                if handledCounter > 0:
                                    logging.warning(f"Message for topic '{msg.topic}' handle {handledCounter} times")

                                handledCounter += 1

                                logging.debug(f'Send to db: {db_insert}')
                                try:
                                    if not rule.get('disable_write', False):
                                        self._influxdb.write(db_inserts)
                                    else:
                                        logging.info(f"Not writing: {db_inserts}")
                                except Exception as e:
                                    logging.error(f'Could not insert into db: {e}')

                self._mqtt.getQueue().task_done()
            except Exception as e:
                logging.error(f'Error while sending from mqtt to db: {type(e).__name__}: {e}')

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

            # Add topic to list of normalized Topics
            if topicObject.normalized not in self._normalizedTopics:
                self._normalizedTopics[topicObject.normalized] = []

            self._normalizedTopics[topicObject.normalized].append(rule)

            # Add tokens to topic
            if ('tokens' in rule) and isinstance(rule['tokens'], dict):
                for tokenName, tokenData in rule['tokens'].items():
                    if 'rule' in tokenData:
                        topicObject.addTokenRule(tokenName, tokenData['rule'])

    def _subcribeMqttTopics(self):
        for normalizedTopic in self._normalizedTopics:
            self._mqtt.subscribe(normalizedTopic)

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
