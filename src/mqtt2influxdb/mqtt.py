import asyncio
import logging
import time
from dataclasses import dataclass

import aiomqtt


@dataclass
class MqttMessage:
    topic: str
    payload: bytes
    qos: int
    retain: bool


class Mqtt:
    username = ""
    password = ""
    address = "localhost"
    port = 1883
    prefix = ""

    _client = None
    _queue = None
    _topics = None

    def __init__(self, config):
        if config is None:
            raise ValueError("No configuration given.")

        mqttConfig = config.get("mqtt", None)

        if mqttConfig is None:
            raise ValueError("No configuration section for MQTT")

        self.username = mqttConfig.get("username", "")
        self.password = mqttConfig.get("password", "")
        self.address = mqttConfig.get("address", "localhost")
        self.port = mqttConfig.get("port", 1883)
        self.prefix = mqttConfig.get("prefix", "") or ""

        if self.prefix and not self.prefix.endswith("/"):
            self.prefix += "/"

        # Optional warning threshold for the internal backlog.  Set to 0 to
        # disable the warning.
        self.queue_warning_size = mqttConfig.get("queue_warning_size", 1000)
        self.queue_warning_interval = mqttConfig.get("queue_warning_interval", 10)

        self._queue = asyncio.Queue()
        self._topics = set()
        self._queue_warned = False
        self._last_queue_warning_time = 0

    async def run(self, rule_handler):
        """Connect to the broker, subscribe, and forward messages until cancelled."""
        logging.info("Connecting to MQTT server %s:%s ...", self.address, self.port)
        async with aiomqtt.Client(
            hostname=self.address,
            port=self.port,
            username=self.username or None,
            password=self.password or None,
            keepalive=60,
        ) as self._client:
            logging.info("Connected to MQTT server %s:%s.", self.address, self.port)

            await rule_handler.subscribe_topics()

            warning_task = asyncio.create_task(self._queue_warning_loop())
            try:
                await self._consume_messages()
            finally:
                warning_task.cancel()
                try:
                    await warning_task
                except asyncio.CancelledError:
                    pass

    async def _consume_messages(self):
        async for message in self._client.messages:
            await self._handle_message(message)

    async def _handle_message(self, message):
        received_at = time.time_ns()
        topic_name = str(message.topic)
        logging.debug("Message: %s %s", topic_name, message.payload)

        # Capture wall-clock time as early as possible so we can stamp the
        # InfluxDB point with the moment the MQTT message arrived, not the
        # moment it is eventually written.
        if topic_name.startswith(self.prefix):
            topic_name = topic_name[len(self.prefix):]
        else:
            logging.error("Received message does not contain prefix.")
            return

        await self._queue.put(
            (
                MqttMessage(
                    topic=topic_name,
                    payload=message.payload,
                    qos=message.qos,
                    retain=message.retain,
                ),
                received_at,
            )
        )
        self._check_queue_size()

    async def _queue_warning_loop(self):
        while True:
            await asyncio.sleep(self.queue_warning_interval)
            self._check_queue_size(force=True)

    async def subscribe(self, topic):
        full_topic = self.prefix + topic
        logging.info("Subscribing to %s.", full_topic)
        self._topics.add(topic)
        await self._client.subscribe(full_topic)

    async def publish(self, topic, value, retain):
        full_topic = self.prefix + topic
        logging.debug("Publishing to '%s': %r", full_topic, value)
        await self._client.publish(topic=full_topic, payload=value, qos=0, retain=retain)

    def getQueue(self):
        return self._queue

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

    async def disconnect(self):
        """Hook for explicit cleanup; the async context manager disconnects the client."""
        logging.info("Disconnecting from MQTT server ...")
        self._client = None
