import unittest
import topic

class TopicTests(unittest.TestCase):

    TEST_DATA = {
        '#': {
            'normalized': '#',
            'parse': [
                {
                    'topic': '/a',
                    'result': {}
                },
                {
                    'topic': '/a/b',
                    'result': {},
                },
            ],
        },
        '+': {
            'normalized': '+',
            'parse': [
                {
                    'topic': '/a',
                    'result': {}
                },
            ],
        },
        '/': {
            'normalized': '/',
        },
        '/test': {
            'normalized': '/test',
        },
        '/+a/b': {
            'normalized': '/+/b',
            'parse': [
                {
                    'topic': '/testA/b',
                    'result': {'a': 'testA'}
                },
            ],
        },
        '/+a/#': {
            'normalized': '/+/#',
            'parse': [
                {
                    'topic': '/testA/b',
                    'result': {'a': 'testA'}
                },
                {
                    'topic': '/testA/b',
                    'rules': {'a': 'testA'},
                    'result': {'a': 'testA'}
                },
                {
                    'topic': '/testA/b',
                    'rules': {'a': 'testNotA'},
                    'result': None
                },
            ],
        },
        '/+a/#b': {
            'normalized': '/+/#',
            'parse': [
                {
                    'topic': '/testA/testB',
                    'result': {'a': 'testA', 'b': ['testB']}
                },
            ],
        },
        'a/+b/#c': {
            'normalized': 'a/+/#',
            'parse': [
                {
                    'topic': 'a/testB/testC1/testC2',
                    'result': {'b': 'testB', 'c': ['testC1', 'testC2']}
                },
                {
                    'topic': 'a/testB',
                    'result': {'b': 'testB', 'c': []}
                },
                {
                    'topic': 'a/testB/testC',
                    'result': {'b': 'testB', 'c': ['testC']}
                },
            ]
        },
        'a/+/+/d/+e/#f': {
            'normalized': 'a/+/+/d/+/#',
            'parse': [
                {
                    'topic': 'a/b/c/d/testE/testF1/testF2',
                    'result': {'e': 'testE', 'f': ['testF1', 'testF2']}
                },
            ]
        },
        '+raum/#': {
            'parse': [
                {
                    'topic': 'arbeitszimmer-tobias/sensor_magnet.aq2/voltage',
                    'result': {'raum': 'arbeitszimmer-tobias'},
                }
            ]
        }
    };

    def testNormalized(self):
        for key, value in self.TEST_DATA.items():
            if 'normalized' not in value:
                continue

            topicObject = topic.Topic(key)

            self.assertEqual(value['normalized'], topicObject.normalized)

    def testParse(self):
        for key, value in self.TEST_DATA.items():
            if 'parse' not in value:
                continue

            topicObject = topic.Topic(key)

            for parseEntry in value['parse']:
                topicObject.clearTokenRules();
                if ('rules' in parseEntry):
                    for tokenName, tokenData in parseEntry['rules'].items():
                        topicObject.addTokenRule(tokenName, tokenData)
                self.assertEqual(topicObject.parse(parseEntry['topic']), parseEntry['result'])
