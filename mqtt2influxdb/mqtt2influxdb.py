#!/usr/bin/python

import logging
import sys
import daemon
import time
import argparse
import yaml
import threading
import re
import paho.mqtt

from . import influxdb_ as influxdb
from . import mqtt
from . import ruleHandler


def main(argv=None):
    if argv is None:
        argv = sys.argv

    args = parseArgs(argv)

    if (args.daemon):
        context = daemon.DaemonContext()

        with context:
            run(args)
    else:
        run(args)

def parseArgs(argv):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=True
        )

    parser.add_argument("-c", "--conf_file",
                        help="Specify config file", metavar="FILE", required = True)

    parser.add_argument("-d", "--daemon",
                        help="Run as daemon", action='store_true')

    parser.add_argument("-v", "--verbose",
                        help="Increases log verbosity for each occurence", dest="verbose_count", action="count", default=0)

    args = parser.parse_args()

    return args

def parseConfig(filename):
    try:
        return yaml.safe_load(open(filename, "r"))
    except Exception as e:
        raise
        logging.error("Can't load yaml file %r (%r)" % (filename, e))

def run(args):
    logging.basicConfig(format="%(asctime)s [%(threadName)-15s] %(levelname)-6s %(message)s",
                        level=max(3 - args.verbose_count, 0) * 10)

    config = parseConfig(args.conf_file)

    m = mqtt.Mqtt(config)
    m.connect()

    db = influxdb.Influxdb(config)
    db.connect()

    rh = ruleHandler.RuleHandler(config, m, db)

    stopEvent = threading.Event()

    try:
        while True:
            stopEvent.wait(60)

    except (SystemExit,KeyboardInterrupt):
        # Normal exit getting a signal from the parent process
        pass
    except:
        # Something unexpected happened?
        logging.exception("Exception")
    finally:
        logging.info("Shutting down...")

        stopEvent.set()

        rh.finish()
        m.disconnect()
        db.disconnect()

        logging.shutdown()

if __name__ == "__main__":
    main()
