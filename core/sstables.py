from sstable import SSTable


class SSTables(object):
    managed : list[SSTable]

    def __init__(self, filename_prefix: str):
        self.count = 0
        self.managed = []
        self.filename_prefix = filename_prefix

    def generate_new_filename(self) -> str:
        # TODO zfill should be a bit more dynamic than this, but this is just lazy b/c whatever
        filename = self.filename_prefix + str(self.count).zfill(5) + ".json"
        self.count += 1
        return filename

    def add(self, sstable: SSTable):
        self.managed.insert(0, sstable)

    def tables(self) -> list[SSTable]:
        return self.managed
