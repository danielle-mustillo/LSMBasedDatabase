import os
import time
from pathlib import Path
from typing import Any

from item import Item
from memtable import MemTable
from sstable import SSTables, SSTable
from wal import WriteAheadLog


def create_database(operation_folder: str, wal_filename: str, sstable_filename_prefix: str, page_size: int):
    Path(operation_folder).mkdir(parents=True, exist_ok=True)
    delete_files_in_directory(operation_folder)
    return Database(operation_folder, wal_filename, sstable_filename_prefix, page_size)


def delete_files_in_directory(directory):
    # Get all files in the directory
    file_list = os.listdir(directory)

    # Iterate over all files and delete each one
    for file_name in file_list:
        file_path = os.path.join(directory, file_name)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Deleted {file_path}")
        except Exception as e:
            print(f"Error deleting {file_path}: {e}")

class Database(object):

    def __init__(self, operation_folder: str, wal_filename: str, sstable_filename_prefix: str, page_size: int):
        self.operation_folder = operation_folder
        self.page_size = page_size
        self.sstables = SSTables(operation_folder + sstable_filename_prefix)
        self.memtable = MemTable()
        self.wal = WriteAheadLog(operation_folder + wal_filename)

    def insert(self, key: str, value: str):
        item = Item(key=key, value=value, is_deleted=False, timestamp=time.time_ns())
        self.alter_db(item)

    def update(self, key: str, value: str):
        self.insert(key=key, value=value)

    def delete(self, key: str):
        # None is the tombstone marker
        item = Item(key=key, value=None, is_deleted=True,  timestamp=time.time_ns())
        self.alter_db(item)

    def get(self, key: str) -> str | None:
        # first search in the memtable if its there, if its there, take the latest value there.
        item = self.memtable.get(key)
        if item is not None:
            return self.cleanup_return(item)

        # if not in memtable, go through sstables
        for sstable in self.sstables.tables():
            item = sstable.search(key)
            if item is not None:
                return self.cleanup_return(item)

        return None

    def cleanup_return(self, item: Item) -> str | None:
        if item is None:
            return None
        if item.is_deleted:
            return None
        return item.value



    def alter_db(self, item: Item):
        # Always push to memtable
        self.memtable.insert(item)
        # self.wal.insert(item)

        # if the memtable is too big, we gonna push it to the sstable
        # Todo maybe we can play with concurrency... doing this in the background maybe?
        if self.memtable.size() >= self.page_size:
            filename = self.sstables.generate_new_filename()
            ss_table = SSTable(self.memtable, filename)
            self.sstables.add(ss_table)
            self.memtable.clear()
