from flask import Flask, request
from flask_restful import Resource, Api
from utils import *

app = Flask(__name__)
api = Api(app)


class GetAndPostMessage(Resource):
    def post(self):

        try:
            message = request.get_json()
            print(message)
            buoy_id, data = parseData(message)
            postToInflux(data, buoy_id)

            return 200
        except:

            return 400


api.add_resource(GetAndPostMessage, '/')

if __name__ == '__main__':
    app.run(debug=True)
