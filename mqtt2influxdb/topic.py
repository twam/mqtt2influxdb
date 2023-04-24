import re

class Topic:
    """
        This class is inspired by code from
            https://github.com/RangerMauve/mqtt-regex/blob/master/index.js
        which is published under the following license:

        The MIT License (MIT)

        Copyright (c) 2014 RangerMauve

        Permission is hereby granted, free of charge, to any person obtaining a copy
        of this software and associated documentation files (the "Software"), to deal
        in the Software without restriction, including without limitation the rights
        to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
        copies of the Software, and to permit persons to whom the Software is
        furnished to do so, subject to the following conditions:

        The above copyright notice and this permission notice shall be included in all
        copies or substantial portions of the Software.

        THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
        IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
        FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
        AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
        LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
        OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
        SOFTWARE.
    """

    def __init__(self, topic):
        self._topic = topic
        self._tokenRules = {}
        self._createTokens()
        self._calculateNormalized()
        self._makeRegex()

    @property
    def topic(self):
        return self._topic

    @property
    def normalized(self):
        return self._normalized

    @property
    def tokenRules(self):
        return self._tokenRules

    def _calculateNormalized(self):
        self._normalized = '/'.join(list(map(
            lambda token : 
                token['piece'][0:-1] if (token['type'] == "raw") else
                "+" if (token['type'] == "single") else 
                "#" if (token['type'] == "multi") else
                ""
        , self._tokens)))

    def parse(self, topic):
        matches = self._regex.match(topic)

        # Return empty set if regex did not match anything
        if (not matches):
            return {};

        # Get the capturing tokens
        captureTokens = list(filter(lambda x : x['type'] != 'raw', self._tokens))

        res = {}

        # Iterate over all matches
        for index, capture in enumerate(matches.groups()):
            token = captureTokens[index]
            param = capture

            # Skip tokens which do not have a name
            if ('name' not in token) or (token['name'] == ''):
                continue;

            # Multi tokens should return an array of of items, split them along the '/'
            if (token['type'] == 'multi'):
                param = capture.split('/')

                # If last element of array is empty, remove it
                if (param[-1] == ""):
                    param = param[0:-1]

            # If last element of single token is '/', remove it
            elif (capture[-1] == '/'):
                param = capture[0:-1]

            # Check for rules (for single tokens)
            if (token['type'] == 'single') and (token['name'] in self._tokenRules):
                if (not self._tokenRules[token['name']].match(param)):
                    #print('%r did not match on %s' % (self._tokenRules[token['name']].pattern, param))
                    return None

            res[token['name']] = param

        return res;

    def clearTokenRules(self):
        self._tokenRules = {}

    def addTokenRule(self, token, data):
        self._tokenRules.update({token: re.compile(data)})

    def _createTokens(self):
        self._tokens = list(map(self._processToken, self._topic.split('/')))

    def _processToken(self, token):
        if ((len(token)>0) and (token[0] == '+')):
            return self._processTokenSingle(token)
        elif ((len(token)>0) and (token[0] == '#')):
            return self._processTokenMulti(token)
        else:
            return self._processTokenRaw(token)

    def _processTokenMulti(self, token):
        return {
            'type': 'multi',
            'name': token[1:],
            'piece': '((?:[^/#+]+/)*)',
            'last': '((?:[^/#+]+/?)*)'
            }

    def _processTokenSingle(self, token):
        return {
            'type': 'single',
            'name': token[1:],
            'piece': '([^/#+]+/)',
            'last': '([^/#+]+/?)'
            }

    def _processTokenRaw(self, token):
        escapedToken = re.escape(token)

        return {
            'type': 'raw',
            'piece': escapedToken + '/',
            'last': escapedToken + '/?'
            }

    def _makeRegex(self):
        pattern = '^'

        for index, token in enumerate(self._tokens):
            isLast = (index == len(self._tokens) - 1)
            beforeMulti = (index == len(self._tokens) - 2) and (self._tokens[-1]['type'] == 'multi')
            pattern += token['last'] if (isLast or beforeMulti) else token['piece']

        pattern += '$'

        self._regex = re.compile(pattern)