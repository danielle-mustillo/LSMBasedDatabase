from item import Item


class MemTable(object):
    memtable : list[Item]

    def __init__(self):
        self.memtable = []
        self.item_count = 0

    # TODO we can make this an LSM tree I think....

    def insert(self, item: Item):

        # All items in the same page will receive a count of when it was inserted.
        # If two items in the same page have the same timestamp and the same key
        # We can use the item count to resolve which one is "more upto date"
        item.set_item_count(self.item_count)
        self.item_count += 1
        self.memtable.insert(0, item)

    def size(self):
        return len(self.memtable)

    def items(self) -> list[Item]:
        return self.memtable

    def clear(self):
        self.memtable = []
        self.item_count = 0

    def delete(self, key):
        for item in self.memtable:
            if item.key == key:
                self.memtable.remove(item)

    def get(self, key):
        for item in self.memtable:
            if item.key == key:
                # Latest item added is always the first one we find, in this naive non-lsm tree based implementation.
                return item
        return None
