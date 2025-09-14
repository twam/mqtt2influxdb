import influxdb
import logging

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
        if (config == None):
            raise "No configuration given."

        # Load InfluxDB settings
        influxdbConfig = config.get("influxdb", None)

        if (influxdbConfig == None):
                raise "No configuration section for InfluxDB"

        self.username = influxdbConfig.get("username", "")
        self.password = influxdbConfig.get("password", "")
        self.address = influxdbConfig.get("address", "localhost")
        self.port = influxdbConfig.get("port", 1883)
        self.database =  influxdbConfig.get("database", None)

    def connect(self):
        logging.info("Connecting to InfluxDB server " + self.address + ":" + str(self.port) + " with username '" + self.username + "'")
        self._client = influxdb.InfluxDBClient(self.address, self.port, self.username, self.password, self.database)

    def disconnect(self):
        if hasattr(self._client, "close"):
            logging.info("Disconnecting from InfluxDB server.")
            self._client.close()

    def write(self, message):
        logging.debug("Writing %r to database." % message)
        self._client.write_points(message);