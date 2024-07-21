import pytest

from bloom import BloomFilter


@pytest.fixture
def bloom_filter():
    return BloomFilter()

def test_add_and_check(bloom_filter):
    assert bloom_filter.check("hello") == False
    assert bloom_filter.check("world") == False
    assert bloom_filter.check("everyone") == False
    assert bloom_filter.check("!") == False

    bloom_filter.add("hello")
    assert bloom_filter.check("hello") == True
    assert bloom_filter.check("world") == False
    assert bloom_filter.check("everyone") == False
    assert bloom_filter.check("!") == False

    bloom_filter.add("world")
    assert bloom_filter.check("hello") == True
    assert bloom_filter.check("world") == True
    assert bloom_filter.check("everyone") == False
    assert bloom_filter.check("!") == False

    bloom_filter.add("everyone")
    assert bloom_filter.check("hello") == True
    assert bloom_filter.check("world") == True
    assert bloom_filter.check("everyone") == True
    assert bloom_filter.check("!") == False

def test_are_all_bits_set(bloom_filter):
    assert bloom_filter._are_all_bits_set(0b0010, 0b1010) == True
    assert bloom_filter._are_all_bits_set(0b1010, 0b0010) == False