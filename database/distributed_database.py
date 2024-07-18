import random

from database.database import Database
from database.database_impl import DatabaseImpl


class DistributedDatabase(Database):
    num_of_dbs: int
    databases: list[Database]

    def __init__(self, num_of_dbs: int, num_of_leaders: int, containing_folder: str, page_size: int):
        self.num_of_dbs = num_of_dbs
        self.num_of_leaders = num_of_leaders

        self.databases = []
        for i in range(0, self.num_of_dbs):
            subfolder = "/db" + str(i)
            op_subpath = "/output"
            wal_subpath = "wal"
            op_path = containing_folder + subfolder + op_subpath
            wal_path = containing_folder + subfolder + wal_subpath
            ss_table_prefix = "sstable"
            db = DatabaseImpl(operation_folder=op_path, wal_prefix=wal_path,
                              sstable_filename_prefix=ss_table_prefix, page_size=page_size)
            self.databases.append(db)

        self.leader_databases = []
        for i in range(0, self.num_of_leaders):
            if i % 2 == 0:
                self.leader_databases.append(self.databases[i])



    def insert(self, key: str, value: str):
        return self.choose_leader().insert(key=key, value=value)

    def update(self, key: str, value: str):
        return self.choose_leader().update(key=key, value=value)

    def delete(self, key: str):
        return self.choose_leader().delete(key=key)

    def get(self, key: str) -> str | None:
        return self.choose_any().get(key=key)

    def choose_leader(self):
        choice = random.randint(0, self.num_of_leader_dbs)
        return self.leader_databases[choice]

    def choose_any(self):
        choice = random.randint(0, self.num_of_dbs)
        return self.databases[choice]