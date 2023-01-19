import json


class message:
    """Standard Message format for all communications"""

    def __init__(self, TYPE=None, DATA=None):
        self.TYPE = TYPE
        self.DATA = DATA

    def get_encoded_message(self):
        message = {"T": self.TYPE, "D": self.DATA}
        message = json.dumps(message)
        message = message.encode("UTF-8")
        return message

    def decode_message(self, message):
        message = message.decode("UTF-8")
        message = json.loads(message)
        return message
