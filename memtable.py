from item import Item


class MemTable(object):
    memtable = []
    # TODO we can make this an LSM tree I think....

    def insert(self, item: Item):
        self.memtable.append(item)