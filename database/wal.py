import json

from memtable import Item


class WriteAheadLog(object):

    def __init__(self, filename: str):
        self.filename = filename


    def insert(self, item: Item):
        with open(self.filename, 'a') as file:
            json.dump(item, file)
            file.write('\n')  # Add a newline for separation between appended items
        pass