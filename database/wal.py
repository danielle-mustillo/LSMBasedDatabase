import json
from typing import BinaryIO

import jsonpickle

from memtable import Item


class WriteAheadLog:
    current_file_index: int
    file_size: int
    byte_position: int
    filehandle: BinaryIO
    filename_prefix: str

    def __init__(self, filename_prefix: str):
        self.current_file_index = 0
        self.filename_prefix = filename_prefix
        self.file_size = 1024 * 1024  # 1MB
        self.filehandle = BinaryIO()

        self._allocate_new_file()

    def _allocate_new_file(self):
        old_filehandle = self.filehandle
        filename = self.filename_prefix + str(self.current_file_index) + ".txt"
        self.current_file_index += 1

        # take the time to allocate all the bytes in the file, hopefully contiguously on disk
        self.filehandle = open(filename, 'xb')
        self.filehandle.write(bytes(self.file_size))

        self.filehandle.seek(0, 0)  # set filehandle to 0th byte, whence=0 means absolute position in the file at the start.
        self.byte_position = 0

        old_filehandle.close()

    def close(self):
        self.filehandle.close()


    def insert(self, item: Item):
        frozen = jsonpickle.encode(item, indent=False)
        write = (frozen + '\n').encode()
        if self.byte_position + len(write) >= self.file_size:
            self._allocate_new_file()

        self.filehandle.write(write)  # Add a newline for separation between appended items