from typing import Optional

from bloom import BloomFilter
from item import Item
from memtable import MemTable

import file

import jsonpickle

class SSTable(object):

    def __init__(self, memtable: MemTable, filename: str):
        self.items = memtable.items()

        # sort the strings for sstable
        self.items.sort(key=lambda x: x.key)

        self.bloom = BloomFilter()
        self.bloom.add_table(self.items)

        self.filename = filename

        frozen = jsonpickle.encode(self.items, indent=False)
        file.write(filename, frozen)



    def is_key_in_bloom(self, key):
        return self.bloom.check(key)

    def read(self) -> list[Item]:
        return file.read(self.filename)
        #
        # items = []
        # for val in data:
        #     items.append(Item(key=val['key'], value=val['value'], is_deleted=val['is_deleted'], timestamp=val['timestamp'], perfcounter=val['perfcounter']))
        #
        # return items

    def search(self, key: str) -> Optional[Item]:
        if not self.is_key_in_bloom(key):
            return None

        page = self.read()

        def pick_latest(item: Item, item2: Item) -> Item:
            if item == None:
                return item2
            if item2 == None:
                return item
            if item.timestamp == item2.timestamp:
                if item.item_count > item2.item_count:
                    return item
                else:
                    return item2
            if item.timestamp > item2.timestamp:
                return item
            else:
                return item2

        # Because a page can have many duplicate entries in it, we need to de-duplicate upon read time.
        # The SSTable is sorted by key so we know the neighbouring items in the sstable
        # will have the same key if they are duplicates.
        # So this algorithm just looks at the immediate neighbours and finds the most recent one.
        # It stops iterating in either direction when the key changes traveling left/right.
        def search_neighbours_and_get_latest(page: list[Item], item: Item, idx: int) -> Item:
            left = idx - 1
            right = idx + 1
            latest_found = item
            while left >= 0 and page[left].key == item.key:
                latest_found = pick_latest(latest_found, page[left])
                left -= 1
            while right < len(page) and page[right].key == item.key:
                latest_found = pick_latest(latest_found, page[right])
                right += 1
            return latest_found

        # Binary search through page for our value.
        # This definitely isn't the most optimal implementation, but damn it works so wtv...
        found_idx : Optional[int] = None
        found_item : Optional[Item] = None
        left = 0
        right = len(page) - 1
        while left < right - 1:
            mid = int((left + right) / 2)
            item = page[mid]
            if key == item.key:
                # only take the latest timestamped item
                found_item = item
                found_idx = mid
            if key < item.key:
                right = mid
            else:
                left = mid
        if page[right].key == key:
            item = page[right]
            found_item = item
            found_idx = right
        if page[left].key == key:
            item = page[left]
            found_item = item
            found_idx = left

        if found_item is None:
            return None
        else:
            return search_neighbours_and_get_latest(page, found_item, found_idx)





