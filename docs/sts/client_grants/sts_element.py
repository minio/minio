# -*- coding: utf-8 -*-
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from xml.etree import cElementTree
from xml.etree.cElementTree import ParseError

if hasattr(cElementTree, 'ParseError'):
    _ETREE_EXCEPTIONS = (ParseError, AttributeError, ValueError, TypeError)
else:
    _ETREE_EXCEPTIONS = (SyntaxError, AttributeError, ValueError, TypeError)

_STS_NS = {'sts': 'https://sts.amazonaws.com/doc/2011-06-15/'}


class STSElement(object):
    """STS aware XML parsing class. Wraps a root element name and
    cElementTree.Element instance. Provides STS namespace aware parsing
    functions.

    """

    def __init__(self, root_name, element):
        self.root_name = root_name
        self.element = element

    @classmethod
    def fromstring(cls, root_name, data):
        """Initialize STSElement from name and XML string data.

        :param name: Name for XML data. Used in XML errors.
        :param data: string data to be parsed.
        :return: Returns an STSElement.
        """
        try:
            return cls(root_name, cElementTree.fromstring(data))
        except _ETREE_EXCEPTIONS as error:
            raise InvalidXMLError(
                '"{}" XML is not parsable. Message: {}'.format(
                    root_name, error.message
                )
            )

    def findall(self, name):
        """Similar to ElementTree.Element.findall()

        """
        return [
            STSElement(self.root_name, elem)
            for elem in self.element.findall('sts:{}'.format(name), _STS_NS)
        ]

    def find(self, name):
        """Similar to ElementTree.Element.find()

        """
        elt = self.element.find('sts:{}'.format(name), _STS_NS)
        return STSElement(self.root_name, elt) if elt is not None else None

    def get_child_text(self, name, strict=True):
        """Extract text of a child element. If strict, and child element is
        not present, raises InvalidXMLError and otherwise returns
        None.

        """
        if strict:
            try:
                return self.element.find('sts:{}'.format(name), _STS_NS).text
            except _ETREE_EXCEPTIONS as error:
                raise InvalidXMLError(
                    ('Invalid XML provided for "{}" - erroring tag <{}>. '
                     'Message: {}').format(self.root_name, name, error.message)
                )
        else:
            return self.element.findtext('sts:{}'.format(name), None, _STS_NS)

    def text(self):
        """Fetch the current node's text

        """
        return self.element.text
