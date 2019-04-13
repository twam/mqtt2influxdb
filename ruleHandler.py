import paho.mqtt.client as mqttClient
import logging
import threading
import queue
import re
import json
import topic

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
                            if (msg.retain == True) and (retain == False):
                                logging.debug("Ignore retained message for topic '%s'" % msg.topic)
                                continue

                            matches = topicObject.parse(msg.topic)

                            if (matches != None):
                                db_insert = {
                                    'fields': {},
                                    'tags': {}
                                }

                                if ('payload' in rule):
                                    name = rule['payload'].get('name', 'payload')

                                    if rule['payload'].get('field', False) == True:
                                        db_insert['fields'][name] = self._convertToType(msg.payload.decode("UTF-8"), rule['payload'].get('type', None))

                                    if ('tag' in rule['payload']) and (rule['payload']['tag'] == True):
                                        db_insert['fields'][name] = self._convertToType(msg.payload.decode("UTF-8"), 'string')

                                if ('fields' in rule) and (rule['fields'] != None):
                                    for fieldName, fieldValue in rule['fields'].items():
                                        db_insert['tag'][fieldName] = fieldValue

                                if ('tags' in rule) and (rule['tags'] != None):
                                    for tagName, tagValue in rule['tags'].items():
                                        db_insert['tags'][tagName] = tagValue

                                if rule.get('measurement', None) != None:
                                    db_insert['measurement'] = self._convertToType(rule['measurement'], 'string')

                                for tokenName, tokenValue in matches.items():
                                    if tokenName in rule['tokens']:
                                        tokenConfig = rule['tokens'][tokenName]

                                        if tokenConfig.get('field', False) == True:
                                            db_insert['fields'].update({tokenName: str(tokenValue)})

                                        if tokenConfig.get('tag', False) == True:
                                            db_insert['tags'].update({tokenName: str(tokenValue)})

                                        if tokenConfig.get('measurement', False) == True:
                                            db_insert['measurement'] = tokenValue

                                # Check db_insert
                                if 'measurement' not in db_insert:
                                    logging.error('No measurement for rule %s' % topicObject.topic)

                                if handledCounter > 0:
                                    logging.warning("Message for topic '%s' handle %u times" % (msg.topic, handledCounter))

                                handledCounter += 1

                                logging.debug('Send to db: %r' % (db_insert))
                                try:
                                    self._influxdb.write([db_insert])
                                except Exception as e:
                                    logging.error('Could not insert into db:' + str(e))

                self._mqtt.getQueue().task_done()
            except Exception as e:
                logging.error('Error while sending from mqtt to db: ' + str(e))

    def _parseConfiguration(self, config):
        self._topicObjects = []
        self._normalizedTopics = {}

        # Load Rules
        self._rules = config.get("rules", None)

        if (self._rules == None):
            raise "No configuration section for Rules"

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
            if ('tokens' in rule) and (type(rule['tokens']) == dict):
                for tokenName, tokenData in rule['tokens'].items():
                    if 'rule' in tokenData:
                        topicObject.addTokenRule(tokenName, tokenData['rule'])

    def _subcribeMqttTopics(self):
        for normalizedTopic in self._normalizedTopics:
            self._mqtt.subscribe(normalizedTopic)

    def _convertToType(self, value, type_ = None):
        if type_ == None:
            if re.match("^\d+?\.?\d+?", value):
                return self._convertToType(value, 'float')
            elif re.match("^(true|True|TRUE|false|False|FALSE)$", value):
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
        else:
            raise Exception("Invalid type '%s'" % type_)
