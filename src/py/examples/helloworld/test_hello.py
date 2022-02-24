from unittest import TestCase

from examples.helloworld.hello import hello


class HelloWorldTestCase(TestCase):
    def test_hello_world_succeed(self):
        self.assertEqual(hello(), "Hello World")
