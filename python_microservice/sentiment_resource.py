# -*- coding: utf-8 -*-

"""Main module."""

import atexit
import requests
from flask import Flask, Response
from flask_restful import reqparse, abort, Api, Resource
from sentiment_provider import SentimentProvider


sentiment_provider = SentimentProvider('lresende-elyra:8888', 'spark_python_yarn_cluster')
sentiment_provider.start()

class SentimentResource(Resource):

    def _html_response(self, data):
        resp = Response(data, mimetype='text/html', headers=None)
        resp.status_code = 200
        return resp

    def get(self, business):
        global sentiment_provider
        result = sentiment_provider.calculate_sentiment(business)
        result = result.replace('\n', '<br/>')
        result = ''.join(result)

        return self._html_response(str(result))
        #return result


app = Flask('Sentiment micro service')
api = Api(app)

parser = reqparse.RequestParser()
parser.add_argument('business')

api.add_resource(SentimentResource, '/sentiment/<business>')


if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)

