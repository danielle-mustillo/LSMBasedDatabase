import json
from dataclasses import dataclass
from json import JSONEncoder



@dataclass
class Item(dict):

    def __init__(self, key:str, value:str, is_deleted : bool, timestamp: int):
        self.key = key
        self.value = value
        self.is_deleted = is_deleted
        self.timestamp = timestamp


    def toJSON(self):
        return json.dumps(
            self,
            default=lambda o: o.__dict__,
            sort_keys=True,
            indent=4)

    def set_item_count(self, item_count):
        self.item_count = item_count