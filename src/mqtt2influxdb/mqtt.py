import paho.mqtt.client as mqtt
import logging
import threading
import queue
import copy
import time

class Mqtt:
    username = ""
    password = ""
    address = "localhost"
    port = 1883
    prefix = ""

    _client = None
    _threads = []
    _queue = None
    _topics = set()

    def __init__(self, config):
        if (config == None):
            raise "No configuration given."

        # Load MQTT settings
        mqttConfig = config.get("mqtt", None)

        if (mqttConfig == None):
                raise "No configuration section for MQTT"

        self.username = mqttConfig.get("username", "")
        self.password = mqttConfig.get("password", "")
        self.address = mqttConfig.get("address", "localhost")
        self.port = mqttConfig.get("port", 1883)
        self._queue = queue.Queue()
        self.prefix = mqttConfig.get("prefix")

        if (self.prefix == None):
            self.prefix = ""
        if (self.prefix != "") and (self.prefix[-1] != '/'):
            self.prefix = self.prefix+'/'

        # Optional warning threshold for the internal backlog.  Set to 0 to
        # disable the warning.
        self.queue_warning_size = mqttConfig.get("queue_warning_size", 1000)
        self.queue_warning_interval = mqttConfig.get("queue_warning_interval", 10)
        self._queue_warned = False
        self._last_queue_warning_time = 0
        self._stopEvent = threading.Event()

    def connect(self):
        logging.info("Connecting to MQTT server " + self.address + ":" + str(self.port) + " ...")

        self._client = mqtt.Client()

        if (self.username != "" and self.password != ""):
            self._client.username_pw_set(self.username, self.password)

        self._client.on_message = self._mqtt_on_message
        self._client.on_connect = self._mqtt_on_connect
        self._client.on_disconnect = self._mqtt_on_disconnect
        self._client.on_log = self._mqtt_on_log
        self._client.connect(self.address, self.port, 60)

        mqttLoopThread = threading.Thread(target=self._mqttLoop, name="mqttLoop")
        mqttLoopThread.start()
        self._threads.append(mqttLoopThread)

        queueMonitorThread = threading.Thread(target=self._queueMonitor, name="queueMonitor")
        queueMonitorThread.daemon = True
        queueMonitorThread.start()
        self._threads.append(queueMonitorThread)

    def disconnect(self):
        logging.info("Disconnecting from MQTT server ...")
        self._stopEvent.set()
        self._client.disconnect()
        self._queue.put(None)

        for t in self._threads:
            t.join()

    def getQueue(self):
        return self._queue

    def subscribe(self, topic):
        fullTopic = self.prefix + topic

        logging.info("Subscribing to " + fullTopic + ".")
        self._topics.add(fullTopic)
        self._client.subscribe(fullTopic)

    def publish(self, topic, value, retain):
        fullTopic = self.prefix + topic

        logging.debug("Publishing to '%s': %r" % (fullTopic, value))
        self._client.publish(topic=fullTopic, payload=value, qos=0, retain=retain)

    def _mqttLoop(self):
        logging.debug("Starting MQTT loop ...")
        self._client.loop_forever()

    def _mqtt_on_connect(self, client, userdata, flags, rc):
        logging.info("Connected to MQTT server " + self.address + ":" + str(self.port) + ".")
        for topic in copy.copy(self._topics):
            self.subscribe(topic)

    def _mqtt_on_disconnect(self, client, userdata, rc):
        logging.info("Disconnected from MQTT server.")

    def _check_queue_size(self, force=False):
        if self.queue_warning_size <= 0:
            return

        size = self._queue.qsize()
        if size > self.queue_warning_size:
            now = time.time()
            deadline = self._last_queue_warning_time + self.queue_warning_interval
            if not self._queue_warned or (force and now >= deadline):
                logging.warning(
                    f"MQTT queue backlog is large ({size} messages, "
                    f"threshold {self.queue_warning_size}). InfluxDB writes "
                    f"are falling behind real-time data."
                )
                self._queue_warned = True
                self._last_queue_warning_time = now
        elif size <= self.queue_warning_size // 2:
            if self._queue_warned:
                logging.info(f"MQTT queue backlog recovered ({size} messages).")
                self._queue_warned = False
                self._last_queue_warning_time = 0

    def _queueMonitor(self):
        logging.debug("Starting queue monitor ...")
        while not self._stopEvent.is_set():
            self._stopEvent.wait(self.queue_warning_interval)
            if not self._stopEvent.is_set():
                self._check_queue_size(force=True)

    def _mqtt_on_message(self, client, userdata, msg):
        logging.debug("Message: "+msg.topic +" "+msg.payload.decode('utf-8', errors="replace"))

        # Capture wall-clock time as early as possible so we can stamp the
        # InfluxDB point with the moment the MQTT message arrived, not the
        # moment it is eventually written.
        received_at = time.time_ns()

        if msg.topic.startswith(self.prefix):
            msg.topic = msg.topic[len(self.prefix):].encode('utf-8')
        else:
            raise "Received message does not contain prefix."

        self._queue.put((msg, received_at))
        self._check_queue_size()

    def _mqtt_on_log(self, client, userdata, level, buf):
        if (level == mqtt.MQTT_LOG_ERR):
            logging.error("MQTT: " + buf)
        elif (level == mqtt.MQTT_LOG_WARNING):
            logging.warning("MQTT: " + buf)
        elif ((level == mqtt.MQTT_LOG_INFO) or (level == mqtt.MQTT_LOG_NOTICE)):
            logging.info("MQTT: " + buf)
        else:
            logging.debug("MQTT: " + buf)
