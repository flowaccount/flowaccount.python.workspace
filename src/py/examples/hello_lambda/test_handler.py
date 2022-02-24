import json
from unittest import TestCase

from examples.hello_lambda.handler import hello


class HelloLambdaTestCase(TestCase):
    def test_hello_succeed(self):
        event = {"Hello": "World"}
        resp = hello(event, None)

        self.assertEqual(resp["statusCode"], 200)

        body = json.loads(resp["body"])
        self.assertEqual(
            body["message"], "Go Serverless v1.0! Your function executed successfully!"
        )
        self.assertDictEqual(body["input"], event)
