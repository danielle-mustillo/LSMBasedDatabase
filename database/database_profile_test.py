import cProfile
import io
import pstats
from pstats import SortKey

from faker import Faker
import file
from database import create_database

iterations = 1000
items_to_use = 1000
page_size = 10

def main(setup_function, function_to_profile):
    data = []
    faker = Faker()
    Faker.seed(4321)
    for _ in range(items_to_use):
        data.append({
            "key": faker.name(),
            "value": faker.text()
        })

    database = create_database(operation_folder="output/", wal_filename="wal.txt",
                               sstable_filename_prefix="sorted_string_table", page_size=page_size)

    # Warm up and etc.
    warm_up(data, database)

    setup_function(data, database)

    print("starting profile")
    pr = cProfile.Profile()
    pr.enable()

    function_to_profile(data, database)

    pr.disable()
    s = io.StringIO()
    sortby = SortKey.CUMULATIVE
    ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    ps.print_stats()
    print(s.getvalue())

    print(file.cache_stats())


def warm_up(data, database):
    for i in range(500):
        item = data[i % items_to_use]
        database.insert(item["key"], item["value"])

    for i in range(500):
        item = data[i % items_to_use]
        database.get(item["key"])

    for i in range(500):
        item = data[i % items_to_use]
        database.delete(item["key"])



def test_profile_the_reads():
    def setup(data, database):
        for i in range(iterations):
            item = data[i % items_to_use]
            database.insert(item["key"], item["value"])

    def profile(data, database):
        for i in range(iterations):
            item = data[i % items_to_use]
            database.get(item["key"])

    main(setup_function=setup, function_to_profile=profile)

def test_profile_the_writes():
    def setup(data, database):
        pass
    def profile(data, database):
        for i in range(iterations):
            item = data[i % items_to_use]
            database.insert(item["key"], item["value"])
    main(setup_function=setup, function_to_profile=profile)
