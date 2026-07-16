import influxdb
import logging
import threading


class Influxdb:
    username = ""
    password = ""
    address = "localhost"
    port = 8086
    database = None

    _client = None
    _threads = []
    _queue = None

    def __init__(self, config):
        if config is None:
            raise ValueError("No configuration given.")

        # Load InfluxDB settings
        influxdbConfig = config.get("influxdb", None)

        if influxdbConfig is None:
            raise ValueError("No configuration section for InfluxDB")

        self.username = influxdbConfig.get("username", "")
        self.password = influxdbConfig.get("password", "")
        self.address = influxdbConfig.get("address", "localhost")
        self.port = influxdbConfig.get("port", 1883)
        self.database = influxdbConfig.get("database", None)

        # Batching / flushing options.  A dedicated flush thread sends points to
        # InfluxDB periodically or when the batch is full, so the rule handler
        # does not block on a HTTP round-trip for every single message.
        self.batch_size = influxdbConfig.get("batch_size", 1000)
        self.flush_interval = influxdbConfig.get("flush_interval", 1)
        self.max_buffer_size = influxdbConfig.get("max_buffer_size", 10000)

        self._buffer_lock = threading.Lock()
        self._buffer = []
        self._flush_event = threading.Event()
        self._stop_event = threading.Event()
        self._flush_thread = None

    def connect(self):
        logging.info("Connecting to InfluxDB server " + self.address + ":" + str(self.port) + " with username '" + self.username + "'")
        self._client = influxdb.InfluxDBClient(self.address, self.port, self.username, self.password, self.database)

        self._stop_event.clear()
        self._flush_event.clear()
        self._flush_thread = threading.Thread(target=self._flush_loop, name="influxdbFlush")
        self._flush_thread.start()
        self._threads.append(self._flush_thread)

    def disconnect(self):
        logging.info("Disconnecting from InfluxDB server ...")

        self._stop_event.set()
        self._flush_event.set()

        if self._flush_thread is not None:
            self._flush_thread.join()

        # Flush any remaining points before closing the client
        self._flush()

        if hasattr(self._client, "close"):
            self._client.close()

    def write(self, points):
        if not points:
            return

        with self._buffer_lock:
            self._buffer.extend(points)
            buffer_size = len(self._buffer)

        logging.debug("Buffering %s point(s) for InfluxDB.", len(points))

        if buffer_size >= self.batch_size:
            self._flush_event.set()

        # Backpressure: if the buffer keeps growing because InfluxDB can't keep
        # up, flush synchronously to avoid unbounded memory growth.
        if buffer_size >= self.max_buffer_size:
            logging.warning(
                "InfluxDB buffer reached %s points (max %s); applying backpressure.",
                buffer_size,
                self.max_buffer_size,
            )
            self._flush()

    def _flush_loop(self):
        logging.debug("Starting InfluxDB flush loop ...")

        while not self._stop_event.is_set():
            self._flush_event.wait(self.flush_interval)
            self._flush_event.clear()
            self._flush()

    def _flush(self):
        with self._buffer_lock:
            batch = self._buffer
            self._buffer = []

        if not batch:
            return

        try:
            logging.debug("Flushing %s points to InfluxDB.", len(batch))
            self._client.write_points(batch)
        except Exception as e:
            logging.error("Could not write %s points to InfluxDB: %s", len(batch), e)
