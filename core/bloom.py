import sys

from item import Item


class BloomFilter(object):
    bloom: int = 0
    bloom2: int = 0
    bloom3: int = 0
    bloom_size: int = 64

    def check(self, key: str) -> bool:
        val, val2, val3 = self._compute(key)
        return (val & self.bloom) == val and (val2 & self.bloom2) == val2 and (val3 & self.bloom3) == val3

    def add_table(self, items: list[Item]):
        for key in map(lambda x: x.key, items):
            self.add(key)

    def add(self, key: str):
        computed, computed2, computed3 = self._compute(key)

        self.bloom = self.bloom | computed
        self.bloom2 = self.bloom2 | computed2
        self.bloom3 = self.bloom3 | computed3


    def _are_all_bits_set(self, subset, superset):
        return (subset & superset) == subset

    def _compute(self, key: str) -> [int,int,int]:
        return self._compute_internal(key), self._compute_internal(key[::-1]), self._compute_internal(key + "abc")

    def _compute_internal(self, key: str) -> int:
        check = self._jenkins_one_at_a_time_hash(key.encode('utf-8')) % self.bloom_size
        val = 1 << check
        return val

    # Own internal hash function, because why not.
    # Jenkins hash one at a time implementation
    # Adapted from Wikipedia
    def _jenkins_one_at_a_time_hash(self, key: bytes):
        hash = 0
        for byte in key:
            hash += byte
            hash += hash << 10
            hash ^= hash >> 6
        hash += hash << 3
        hash ^= hash >> 11
        hash += hash << 15
        return hash & 0xFFFFFFFFFFFFFFFF  # Ensure the result is a 64-bit unsigned integer
