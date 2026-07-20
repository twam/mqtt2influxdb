#!/usr/bin/python

import argparse
import asyncio
import logging
import signal
import sys

import daemon
import yaml

from . import influxdb_ as influxdb
from . import mqtt
from .rule_handler import RuleHandler


def main(argv=None):
    if argv is None:
        argv = sys.argv

    args = parseArgs(argv)

    if args.daemon:
        context = daemon.DaemonContext()
        with context:
            asyncio.run(run_async(args))
    else:
        asyncio.run(run_async(args))


def parseArgs(argv):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=True
        )

    parser.add_argument("-c", "--conf_file",
                        help="Specify config file", metavar="FILE", required=True)

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
        logging.error("Can't load yaml file %r (%r)", filename, e)
        raise


async def run_async(args):
    logging.basicConfig(format="%(asctime)s [%(threadName)-15s] %(levelname)-6s %(message)s",
                        level=max(3 - args.verbose_count, 0) * 10)

    config = parseConfig(args.conf_file)

    m = mqtt.Mqtt(config)
    db = influxdb.Influxdb(config)
    rh = RuleHandler(config, m, db)

    db.connect()

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler():
        logging.info("Received shutdown signal.")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler)

    mqtt_task = asyncio.create_task(m.run(rh))
    rh_task = asyncio.create_task(rh.run())
    stop_task = asyncio.create_task(stop_event.wait())
    tasks = [mqtt_task, rh_task, stop_task]

    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        # Cancel the signal watcher and the MQTT task first so no new
        # messages are received.
        if not stop_task.done():
            stop_task.cancel()
        if not mqtt_task.done():
            mqtt_task.cancel()
        await asyncio.gather(stop_task, mqtt_task, return_exceptions=True)

        # Let the rule handler drain the backlog and finish.
        rh.finish()
        await m.getQueue().put(None)
        if not rh_task.done():
            rh_task.cancel()
        await asyncio.gather(rh_task, return_exceptions=True)

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)

        await m.disconnect()
        db.disconnect()
        logging.info("Shutdown complete.")
        logging.shutdown()

    for task in tasks:
        exc = task.exception()
        if exc is not None and not isinstance(exc, asyncio.CancelledError):
            raise exc


if __name__ == "__main__":
    main()
