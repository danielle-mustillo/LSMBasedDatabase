from database import create_database


# content of test_sample.py
def func(x):
    return x + 1


def test_sstable_deletes():
    database = create_database(operation_folder="output/", wal_prefix="wal", sstable_filename_prefix="sstable", page_size=5)
    database.insert("hi0", "hello0")
    database.insert("hi1", "hello1")
    database.insert("hi2", "hello2")
    database.insert("hi3", "hello3")
    database.insert("hi4", "hello4")
    database.insert("hi5", "hello5")
    database.insert("hi6", "hello6")
    database.insert("hi7", "hello7")
    database.insert("hi8", "hello8")
    database.insert("hi9", "hello9")
    database.insert("hi10", "hello10")
    database.insert("hi11", "hello11")

    database.delete("hi0")
    database.delete("hi1")
    database.delete("hi2")
    database.delete("hi3")
    database.delete("hi4")
    database.delete("hi5")

    assert database.get("hi0") is None
    assert database.get("hi1") is None
    assert database.get("hi2") is None
    assert database.get("hi3") is None
    assert database.get("hi4") is None
    assert database.get("hi5") is None
    assert database.get("hi6") == "hello6"
    assert database.get("hi7") == "hello7"
    assert database.get("hi8") == "hello8"
    assert database.get("hi9") == "hello9"
    assert database.get("hi10") == "hello10"
    assert database.get("hi11") == "hello11"

    database.delete("hi6")
    database.delete("hi7")
    database.delete("hi8")
    database.delete("hi9")
    database.delete("hi10")
    database.delete("hi11")

    assert database.get("hi0") is None
    assert database.get("hi1") is None
    assert database.get("hi2") is None
    assert database.get("hi3") is None
    assert database.get("hi4") is None
    assert database.get("hi5") is None
    assert database.get("hi6") is None
    assert database.get("hi7") is None
    assert database.get("hi8") is None
    assert database.get("hi9") is None
    assert database.get("hi10") is None
    assert database.get("hi11") is None



def test_sstable_updates():
    database = create_database(operation_folder="output/", wal_prefix="wal", sstable_filename_prefix="sstable", page_size=5)
    database.insert("hi0", "hello0")
    database.insert("hi1", "hello1")
    database.insert("hi2", "hello2")
    database.insert("hi3", "hello3")
    database.insert("hi4", "hello4")
    database.insert("hi5", "hello5")
    database.insert("hi6", "hello6")
    database.insert("hi7", "hello7")
    database.insert("hi8", "hello8")
    database.insert("hi9", "hello9")
    database.insert("hi10", "hello10")
    database.insert("hi11", "hello11")

    database.insert("hi0", "bonjour0")
    database.update("hi1", "bonjour1")
    database.update("hi2", "bonjour2")
    database.update("hi3", "bonjour3")
    database.update("hi4", "bonjour4")
    database.update("hi5", "bonjour5")
    database.update("hi6", "bonjour6")
    database.update("hi7", "bonjour7")
    database.update("hi8", "bonjour8")
    database.update("hi9", "bonjour9")
    database.update("hi10", "bonjour10")
    database.update("hi11", "bonjour11")

    assert database.get("hi0") == "bonjour0"
    assert database.get("hi1") == "bonjour1"
    assert database.get("hi2") == "bonjour2"
    assert database.get("hi3") == "bonjour3"
    assert database.get("hi4") == "bonjour4"
    assert database.get("hi5") == "bonjour5"
    assert database.get("hi6") == "bonjour6"
    assert database.get("hi7") == "bonjour7"
    assert database.get("hi8") == "bonjour8"
    assert database.get("hi9") == "bonjour9"
    assert database.get("hi10") == "bonjour10"
    assert database.get("hi11") == "bonjour11"



def test_sstable_insertions():
    database = create_database(operation_folder="output/", wal_prefix="wal", sstable_filename_prefix="sstable", page_size=5)
    database.insert("hi0", "hello0")
    database.insert("hi1", "hello1")
    database.insert("hi2", "hello2")
    database.insert("hi3", "hello3")
    database.insert("hi4", "hello4")
    database.insert("hi5", "hello5")
    database.insert("hi6", "hello6")
    database.insert("hi7", "hello7")
    database.insert("hi8", "hello8")
    database.insert("hi9", "hello9")
    database.insert("hi10", "hello10")
    database.insert("hi11", "hello11")

    assert database.get("hi0") == "hello0"
    assert database.get("hi1") == "hello1"
    assert database.get("hi2") == "hello2"
    assert database.get("hi3") == "hello3"
    assert database.get("hi4") == "hello4"
    assert database.get("hi5") == "hello5"
    assert database.get("hi6") == "hello6"
    assert database.get("hi7") == "hello7"
    assert database.get("hi8") == "hello8"
    assert database.get("hi9") == "hello9"
    assert database.get("hi10") == "hello10"
    assert database.get("hi11") == "hello11"



def test_get():
    database = create_database(operation_folder="output/", wal_prefix="wal", sstable_filename_prefix="sstable", page_size=5)
    database.insert("hi0", "hello")

    assert database.get("hi0") == "hello"


def test_update():

    database = create_database(operation_folder="output/", wal_prefix="wal", sstable_filename_prefix="sstable", page_size=5)
    database.insert("hi0", "hello")
    database.update("hi0", "bonjour")

    assert database.get("hi0") == "bonjour"


def test_get_on_nothing():
    database = create_database(operation_folder="output/", wal_prefix="wal", sstable_filename_prefix="sstable", page_size=5)
    database.insert("hi0", "hello")

    assert database.get("hi2") is None


def test_deletes_and_gets():
    database = create_database(operation_folder="output/", wal_prefix="wal", sstable_filename_prefix="sstable", page_size=5)
    database.insert("hi0", "hello")
    database.delete("hi0")

    assert database.get("hi0") is None


def test_insertions():
    database = create_database(operation_folder="output/", wal_prefix="wal", sstable_filename_prefix="sstable", page_size=5)
    database.insert("hi0", "hello")
    database.insert("hi1", "hello")
    database.insert("hi2", "hello")
    database.delete("hi2")
    database.delete("hi1")
    database.delete("hi0")
    database.insert("hi3", "hello")
    database.insert("hi4", "hello")
    database.insert("hi5", "hello")
    database.insert("hi6", "hello")
    database.update("hi5", "bonjour")
    database.insert("hi7", "hello")
    database.insert("hi8", "hello")
    database.update("hi5", "ciao")
    database.insert("hi9", "hello")
    database.insert("hi10", "hello")
    database.update("hi7", "ciao")



    assert database.get("hi0") is None
    assert database.get("hi1") is None
    assert database.get("hi2") is None
    assert database.get("hi3") == "hello"
    assert database.get("hi4") == "hello"
    assert database.get("hi5") == "ciao"
    assert database.get("hi6") == "hello"
    assert database.get("hi7") == "ciao"
    assert database.get("hi8") == "hello"
    assert database.get("hi9") == "hello"
    assert database.get("hi10") == "hello"


