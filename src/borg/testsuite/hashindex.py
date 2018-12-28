import base64
import hashlib
import io
import os
import struct
import tempfile
import zlib

from ..hashindex import NSIndex, ChunkIndex, ChunkIndexEntry
from .. import hashindex
from ..crypto.file_integrity import IntegrityCheckedFile, FileIntegrityError
from . import BaseTestCase

# Note: these tests are part of the self test, do not use or import py.test functionality here.
#       See borg.selftest for details. If you add/remove test methods, update SELFTEST_COUNT


def H(x):
    # make some 32byte long thing that depends on x
    return bytes('%-0.32d' % x, 'ascii')

def H2(x):
    # like H(x), but with pseudo-random distribution of the output value
    return hashlib.sha256(H(x)).digest()

def H3(x):
    # make a 32byte long thing that has x as the value of the last byte
    # very useful if you want to cause key collisions in tests
    # N.B. a key of all zeros is a marker for empty so we start and end with the number
    # and put some nonzero bytes in the middle
    return struct.pack("@L", x) + struct.pack("@L", 255) * 2 + struct.pack("@L", x)


def reverseH3(key):
    return struct.unpack("@L", key[0:8])[0]


class HashIndexTestCase(BaseTestCase):

    def _generic_test(self, cls, make_value, sha):
        idx = cls()
        self.assert_equal(len(idx), 0)
        # Test set
        for x in range(100):
            idx[H(x)] = make_value(x)
        self.assert_equal(len(idx), 100)
        for x in range(100):
            self.assert_equal(idx[H(x)], make_value(x))
        # Test update
        for x in range(100):
            idx[H(x)] = make_value(x * 2)
        self.assert_equal(len(idx), 100)
        for x in range(100):
            self.assert_equal(idx[H(x)], make_value(x * 2))
        # Test delete
        for x in range(50):
            del idx[H(x)]
        # Test some keys still in there
        for x in range(50, 100):
            assert H(x) in idx
        # Test some keys not there any more
        for x in range(50):
            assert H(x) not in idx
        # Test delete non-existing key
        for x in range(50):
            self.assert_raises(KeyError, idx.__delitem__, H(x))
        self.assert_equal(len(idx), 50)
        idx_name = tempfile.NamedTemporaryFile()
        idx.write(idx_name.name)
        del idx
        # Verify file contents
        with open(idx_name.name, 'rb') as fd:
            self.assert_equal(hashlib.sha256(fd.read()).hexdigest(), sha)
        # Make sure we can open the file
        idx = cls.read(idx_name.name)
        self.assert_equal(len(idx), 50)
        for x in range(50, 100):
            self.assert_equal(idx[H(x)], make_value(x * 2))
        idx.clear()
        self.assert_equal(len(idx), 0)
        idx.write(idx_name.name)
        del idx
        self.assert_equal(len(cls.read(idx_name.name)), 0)

    def test_nsindex(self):
        self._generic_test(NSIndex, lambda x: (x, x),
                           '85f72b036c692c8266e4f51ccf0cff2147204282b5e316ae508d30a448d88fef')

    def test_chunkindex(self):
        self._generic_test(ChunkIndex, lambda x: (x, x, x),
                           'c83fdf33755fc37879285f2ecfc5d1f63b97577494902126b6fb6f3e4d852488')

    def test_resize(self):
        n = 2000  # Must be >= MIN_BUCKETS
        idx_name = tempfile.NamedTemporaryFile()
        idx = NSIndex()
        idx.write(idx_name.name)
        initial_size = os.path.getsize(idx_name.name)
        self.assert_equal(len(idx), 0)
        for x in range(n):
            idx[H(x)] = x, x
        idx.write(idx_name.name)
        self.assert_true(initial_size < os.path.getsize(idx_name.name))
        for x in range(n):
            del idx[H(x)]
        self.assert_equal(len(idx), 0)
        idx.write(idx_name.name)
        self.assert_equal(initial_size, os.path.getsize(idx_name.name))

    def test_iteritems(self):
        idx = NSIndex()
        for x in range(100):
            idx[H(x)] = x, x
        iterator = idx.iteritems()
        all = list(iterator)
        self.assert_equal(len(all), 100)
        # iterator is already exhausted by list():
        self.assert_raises(StopIteration, next, iterator)
        second_half = list(idx.iteritems(marker=all[49][0]))
        self.assert_equal(len(second_half), 50)
        self.assert_equal(second_half, all[50:])

    def test_chunkindex_merge(self):
        idx1 = ChunkIndex()
        idx1[H(1)] = 1, 100, 100
        idx1[H(2)] = 2, 200, 200
        idx1[H(3)] = 3, 300, 300
        # no H(4) entry
        idx2 = ChunkIndex()
        idx2[H(1)] = 4, 100, 100
        idx2[H(2)] = 5, 200, 200
        # no H(3) entry
        idx2[H(4)] = 6, 400, 400
        idx1.merge(idx2)
        assert idx1[H(1)] == (5, 100, 100)
        assert idx1[H(2)] == (7, 200, 200)
        assert idx1[H(3)] == (3, 300, 300)
        assert idx1[H(4)] == (6, 400, 400)

    def test_chunkindex_summarize(self):
        idx = ChunkIndex()
        idx[H(1)] = 1, 1000, 100
        idx[H(2)] = 2, 2000, 200
        idx[H(3)] = 3, 3000, 300

        size, csize, unique_size, unique_csize, unique_chunks, chunks = idx.summarize()
        assert size == 1000 + 2 * 2000 + 3 * 3000
        assert csize == 100 + 2 * 200 + 3 * 300
        assert unique_size == 1000 + 2000 + 3000
        assert unique_csize == 100 + 200 + 300
        assert chunks == 1 + 2 + 3
        assert unique_chunks == 3


class HashIndexExtraTestCase(BaseTestCase):
    """These tests are separate because they should not become part of the selftest.
    """
    @staticmethod
    def extract_positions(index, echo=False):
        if echo:
            print("   key  pos                ideal pos V")
        positions = {}
        for i, (key, value) in enumerate(index.memory_view()):
            if key != None:
                if echo:
                    print("{:6} {:4} {}".format(reverseH3(key), i, value))
                positions[reverseH3(key)] = i
        return positions

    def test_chunk_indexer(self):
        # see _hashindex.c hash_sizes, we want to be close to the max. load
        # because interesting errors happen there.
        key_count = int(65537 * ChunkIndex.MAX_LOAD_FACTOR) - 10
        index = ChunkIndex(key_count)
        all_keys = [hashlib.sha256(H(k)).digest() for k in range(key_count)]
        # we're gonna delete 1/3 of all_keys, so let's split them 2/3 and 1/3:
        keys, to_delete_keys = all_keys[0:(2*key_count//3)], all_keys[(2*key_count//3):]

        for i, key in enumerate(keys):
            index[key] = (i, i, i)
        for i, key in enumerate(to_delete_keys):
            index[key] = (i, i, i)

        for key in to_delete_keys:
            del index[key]
        for i, key in enumerate(keys):
            assert index[key] == (i, i, i)
        for key in to_delete_keys:
            assert index.get(key) is None

        # now delete every key still in the index
        for key in keys:
            del index[key]
        # the index should now be empty
        assert list(index.iteritems()) == []

    def test_backshift_normal(self):
        index = ChunkIndex(5)
        num_buckets = 0
        for _ in index.memory_view():
            num_buckets += 1
        ideal_positions = [0, 0, 1]
        readable = [0, 0+num_buckets, 1]
        print(ideal_positions)
        print (readable)
        keys = [H3(r) for r in readable]
        for i, (ideal, key) in enumerate(zip(ideal_positions, keys)):
            index[key] = (ideal, i, 99)
        print(self.extract_positions(index))
        assert self.extract_positions(index) == {0: 0, num_buckets: 1, 1: 2}
        print("del", readable[1])
        del index[keys[1]]
        print()
        positions = self.extract_positions(index)
        assert positions == {0:0, 1:1}

    def test_backshift_normal1(self):
        index = ChunkIndex(5)
        num_buckets = 0
        for _ in index.memory_view():
            num_buckets += 1
        ideal_positions = [0, 0, 2, 0]
        readable = [0, 0+num_buckets, 2, 0+(num_buckets*2)]
        print(ideal_positions)
        print (readable)
        keys = [H3(r) for r in readable]
        for i, (ideal, key) in enumerate(zip(ideal_positions, keys)):
            index[key] = (ideal, i, 99)
        print(self.extract_positions(index))
        assert self.extract_positions(index) == {
            0: 0,
            num_buckets: 1,  # will be deleted! should be on 0 but it collided
            2: 2,
            (2*num_buckets):3,  # should move to 1 once that gets deleted
        }
        print("del", readable[1])
        del index[keys[1]]
        print()
        positions = self.extract_positions(index)
        assert positions == {0:0, 2:2, 2*num_buckets: 1}

    def test_backshift_wrap0(self):
        index = ChunkIndex(5)
        num_buckets = 0
        for _ in index.memory_view():
            num_buckets += 1
        print("num_buckets", num_buckets)
        last = num_buckets -1
        ideal_positions = [last, last]
        readable = [last, last+num_buckets]
        print(ideal_positions)
        print(readable)
        keys = [H3(r) for r in readable]
        for i, (ideal, key) in enumerate(zip(ideal_positions, keys)):
            index[key] = (ideal, i, 99)
        print()
        assert self.extract_positions(index, echo=True) == {
            last: last,  #  this will get deleted
            last+num_buckets: 0,  # this should have been in last, but it collided so we got 0
        }
        print("del", readable[0])
        del index[keys[0]]
        positions = self.extract_positions(index, echo=True)
        assert positions == {
            last+num_buckets: last,  # this should have backshifted by one, and wrapped
        }

    def test_backshift_wrap1(self):
        index = ChunkIndex(7)
        num_buckets = 0
        for _ in index.memory_view():
            num_buckets += 1
        print("num_buckets", num_buckets)
        last = num_buckets -1
        ideal_positions = [last, last, 0]
        readable = [last, last+num_buckets, 0]
        print(ideal_positions)
        print(readable)
        keys = [H3(r) for r in readable]
        for i, (ideal, key) in enumerate(zip(ideal_positions, keys)):
            index[key] = (ideal, i, 99)
        print()
        assert self.extract_positions(index, echo=True) == {
            last: last,  #  this will get deleted
            last+num_buckets: 0,  # this should have been in last pos, but it collided so it's in 0
            0: 1,  # 0 ended up here because of the collisions
        }
        print("del", readable[0])
        del index[keys[0]]
        positions = self.extract_positions(index, echo=True)

        assert positions == {
            last+num_buckets: last,  # this should have backshifted by one, and wrapped
            0: 0,  # this should have backshifted to 0
        }

        # if the above is correct these keys should be found as well, but let's still check
        for key in readable[1:]:
            assert H3(key) in index, "missing key {!r}".format(key)

    def test_backshift_wrap2(self):
        index = ChunkIndex(5)
        num_buckets = 0
        for _ in index.memory_view():
            num_buckets += 1
        print("num_buckets", num_buckets)
        last = num_buckets -1
        ideal_positions = [last, last, 1, 0]
        readable = [last, last+num_buckets, 1, 0]
        print(ideal_positions)
        print(readable)
        keys = [H3(r) for r in readable]
        for i, (ideal, key) in enumerate(zip(ideal_positions, keys)):
            index[key] = (ideal, i, 99)
        print()
        assert self.extract_positions(index, echo=True) == {
            last: last,  #  this will get deleted
            last+num_buckets: 0,  # this should have been in last, but it collided so we got 0
            1: 1,  # this is in it's ideal place
            0: 2,  # 0 ended up here because of the collisions
        }
        print("del", readable[0])
        del index[keys[0]]
        positions = self.extract_positions(index, echo=True)
        assert positions == {
            last+num_buckets: last,  # this should have backshifted by one, and wrapped
            1: 1,  # this can't backshift, since it's allready in place
            0: 0,  # this should have backshifted to 0
        }

    # def test_backshift(self):
    #     index = ChunkIndex(5)
    #     ideal = [0, 0, 2, 1, 2]
    #     keys = [H3(0), H3(0+1031), H3(2), H3(1+1031), H3(2+1031)]
    #     for i, (j, key) in enumerate(zip(ideal, keys)):
    #         index[key] = (j, i, i)
    #     print([v[0:2] for (k,v) in list(index.iteritems())])
    #     del index[keys[0]]
    #     print([v[0:2] for (k,v) in list(index.iteritems())])
    #     for i, (j, key) in enumerate(zip(ideal[1:], keys[1:])):
    #         print(">>", (j, i+1), key[-8:])
    #         assert index[key] == (j, i+1, i+1)


class HashIndexSizeTestCase(BaseTestCase):
    def test_size_on_disk(self):
        idx = ChunkIndex()
        assert idx.size() == 18 + 1031 * (32 + 3 * 4)

    def test_size_on_disk_accurate(self):
        idx = ChunkIndex()
        for i in range(1234):
            idx[H(i)] = i, i**2, i**3
        with tempfile.NamedTemporaryFile() as file:
            idx.write(file.name)
            size = os.path.getsize(file.name)
        assert idx.size() == size


class HashIndexRefcountingTestCase(BaseTestCase):
    def test_chunkindex_limit(self):
        idx = ChunkIndex()
        idx[H(1)] = ChunkIndex.MAX_VALUE - 1, 1, 2

        # 5 is arbitray, any number of incref/decrefs shouldn't move it once it's limited
        for i in range(5):
            # first incref to move it to the limit
            refcount, *_ = idx.incref(H(1))
            assert refcount == ChunkIndex.MAX_VALUE
        for i in range(5):
            refcount, *_ = idx.decref(H(1))
            assert refcount == ChunkIndex.MAX_VALUE

    def _merge(self, refcounta, refcountb):
        def merge(refcount1, refcount2):
            idx1 = ChunkIndex()
            idx1[H(1)] = refcount1, 1, 2
            idx2 = ChunkIndex()
            idx2[H(1)] = refcount2, 1, 2
            idx1.merge(idx2)
            refcount, *_ = idx1[H(1)]
            return refcount
        result = merge(refcounta, refcountb)
        # check for commutativity
        assert result == merge(refcountb, refcounta)
        return result

    def test_chunkindex_merge_limit1(self):
        # Check that it does *not* limit at MAX_VALUE - 1
        # (MAX_VALUE is odd)
        half = ChunkIndex.MAX_VALUE // 2
        assert self._merge(half, half) == ChunkIndex.MAX_VALUE - 1

    def test_chunkindex_merge_limit2(self):
        # 3000000000 + 2000000000 > MAX_VALUE
        assert self._merge(3000000000, 2000000000) == ChunkIndex.MAX_VALUE

    def test_chunkindex_merge_limit3(self):
        # Crossover point: both addition and limit semantics will yield the same result
        half = ChunkIndex.MAX_VALUE // 2
        assert self._merge(half + 1, half) == ChunkIndex.MAX_VALUE

    def test_chunkindex_merge_limit4(self):
        # Beyond crossover, result of addition would be 2**31
        half = ChunkIndex.MAX_VALUE // 2
        assert self._merge(half + 2, half) == ChunkIndex.MAX_VALUE
        assert self._merge(half + 1, half + 1) == ChunkIndex.MAX_VALUE

    def test_chunkindex_add(self):
        idx1 = ChunkIndex()
        idx1.add(H(1), 5, 6, 7)
        assert idx1[H(1)] == (5, 6, 7)
        idx1.add(H(1), 1, 2, 3)
        assert idx1[H(1)] == (6, 2, 3)

    def test_incref_limit(self):
        idx1 = ChunkIndex()
        idx1[H(1)] = (ChunkIndex.MAX_VALUE, 6, 7)
        idx1.incref(H(1))
        refcount, *_ = idx1[H(1)]
        assert refcount == ChunkIndex.MAX_VALUE

    def test_decref_limit(self):
        idx1 = ChunkIndex()
        idx1[H(1)] = ChunkIndex.MAX_VALUE, 6, 7
        idx1.decref(H(1))
        refcount, *_ = idx1[H(1)]
        assert refcount == ChunkIndex.MAX_VALUE

    def test_decref_zero(self):
        idx1 = ChunkIndex()
        idx1[H(1)] = 0, 0, 0
        with self.assert_raises(AssertionError):
            idx1.decref(H(1))

    def test_incref_decref(self):
        idx1 = ChunkIndex()
        idx1.add(H(1), 5, 6, 7)
        assert idx1[H(1)] == (5, 6, 7)
        idx1.incref(H(1))
        assert idx1[H(1)] == (6, 6, 7)
        idx1.decref(H(1))
        assert idx1[H(1)] == (5, 6, 7)

    def test_setitem_raises(self):
        idx1 = ChunkIndex()
        with self.assert_raises(AssertionError):
            idx1[H(1)] = ChunkIndex.MAX_VALUE + 1, 0, 0

    def test_keyerror(self):
        idx = ChunkIndex()
        with self.assert_raises(KeyError):
            idx.incref(H(1))
        with self.assert_raises(KeyError):
            idx.decref(H(1))
        with self.assert_raises(KeyError):
            idx[H(1)]
        with self.assert_raises(OverflowError):
            idx.add(H(1), -1, 0, 0)


class HashIndexDataTestCase(BaseTestCase):
    # This bytestring was created with 1.0-maint at c2f9533
    HASHINDEX = b'eJzt0L0NgmAUhtHLT0LDEI6AuAEhMVYmVnSuYefC7AB3Aj9KNedJbnfyFne6P67P27w0EdG1Eac+Cm1ZybAsy7Isy7Isy7Isy7I' \
                b'sy7Isy7Isy7Isy7Isy7Isy7Isy7Isy7Isy7Isy7Isy7Isy7Isy7Isy7Isy7Isy7Isy7Isy7Isy7Isy7Isy7Isy7LsL9nhc+cqTZ' \
                b'3XlO2Ys++Du5fX+l1/YFmWZVmWZVmWZVmWZVmWZVmWZVmWZVmWZVmWZVmWZVmWZVmWZVmWZVmWZVmWZVmWZVn2/+0O2rYccw=='

    def _serialize_hashindex(self, idx):
        with tempfile.TemporaryDirectory() as tempdir:
            file = os.path.join(tempdir, 'idx')
            idx.write(file)
            with open(file, 'rb') as f:
                return self._pack(f.read())

    def _deserialize_hashindex(self, bytestring):
        with tempfile.TemporaryDirectory() as tempdir:
            file = os.path.join(tempdir, 'idx')
            with open(file, 'wb') as f:
                f.write(self._unpack(bytestring))
            return ChunkIndex.read(file)

    def _pack(self, bytestring):
        return base64.b64encode(zlib.compress(bytestring))

    def _unpack(self, bytestring):
        return zlib.decompress(base64.b64decode(bytestring))

    def test_identical_creation(self):
        idx1 = ChunkIndex()
        idx1[H(1)] = 1, 2, 3
        idx1[H(2)] = 2**31 - 1, 0, 0
        idx1[H(3)] = 4294962296, 0, 0  # 4294962296 is -5000 interpreted as an uint32_t

        assert self._serialize_hashindex(idx1) == self.HASHINDEX

    def test_read_known_good(self):
        idx1 = self._deserialize_hashindex(self.HASHINDEX)
        assert idx1[H(1)] == (1, 2, 3)
        assert idx1[H(2)] == (2**31 - 1, 0, 0)
        assert idx1[H(3)] == (4294962296, 0, 0)

        idx2 = ChunkIndex()
        idx2[H(3)] = 2**32 - 123456, 6, 7
        idx1.merge(idx2)
        assert idx1[H(3)] == (ChunkIndex.MAX_VALUE, 6, 7)


class HashIndexIntegrityTestCase(HashIndexDataTestCase):
    def write_integrity_checked_index(self, tempdir):
        idx = self._deserialize_hashindex(self.HASHINDEX)
        file = os.path.join(tempdir, 'idx')
        with IntegrityCheckedFile(path=file, write=True) as fd:
            idx.write(fd)
        integrity_data = fd.integrity_data
        assert 'final' in integrity_data
        assert 'HashHeader' in integrity_data
        return file, integrity_data

    def test_integrity_checked_file(self):
        with tempfile.TemporaryDirectory() as tempdir:
            file, integrity_data = self.write_integrity_checked_index(tempdir)
            with open(file, 'r+b') as fd:
                fd.write(b'Foo')
            with self.assert_raises(FileIntegrityError):
                with IntegrityCheckedFile(path=file, write=False, integrity_data=integrity_data) as fd:
                    ChunkIndex.read(fd)


class HashIndexCompactTestCase(HashIndexDataTestCase):
    def index(self, num_entries, num_buckets):
        index_data = io.BytesIO()
        index_data.write(b'BORG_IDX')
        # num_entries
        index_data.write(num_entries.to_bytes(4, 'little'))
        # num_buckets
        index_data.write(num_buckets.to_bytes(4, 'little'))
        # key_size
        index_data.write((32).to_bytes(1, 'little'))
        # value_size
        index_data.write((3 * 4).to_bytes(1, 'little'))

        self.index_data = index_data

    def index_from_data(self):
        self.index_data.seek(0)
        index = ChunkIndex.read(self.index_data)
        return index

    def index_to_data(self, index):
        data = io.BytesIO()
        index.write(data)
        return data.getvalue()

    def index_from_data_compact_to_data(self):
        index = self.index_from_data()
        index.compact()
        compact_index = self.index_to_data(index)
        return compact_index

    def write_entry(self, key, *values):
        self.index_data.write(key)
        for value in values:
            self.index_data.write(value.to_bytes(4, 'little'))

    def write_empty(self, key):
        self.write_entry(key, 0xffffffff, 0, 0)

    def write_deleted(self, key):
        self.write_entry(key, 0xfffffffe, 0, 0)

    def test_simple(self):
        self.index(num_entries=3, num_buckets=6)
        self.write_entry(H2(0), 1, 2, 3)
        self.write_deleted(H2(1))
        self.write_empty(H2(2))
        self.write_entry(H2(3), 5, 6, 7)
        self.write_entry(H2(4), 8, 9, 10)
        self.write_empty(H2(5))

        compact_index = self.index_from_data_compact_to_data()

        self.index(num_entries=3, num_buckets=3)
        self.write_entry(H2(0), 1, 2, 3)
        self.write_entry(H2(3), 5, 6, 7)
        self.write_entry(H2(4), 8, 9, 10)
        assert compact_index == self.index_data.getvalue()

    def test_first_empty(self):
        self.index(num_entries=3, num_buckets=6)
        self.write_deleted(H2(1))
        self.write_entry(H2(0), 1, 2, 3)
        self.write_empty(H2(2))
        self.write_entry(H2(3), 5, 6, 7)
        self.write_entry(H2(4), 8, 9, 10)
        self.write_empty(H2(5))

        compact_index = self.index_from_data_compact_to_data()

        self.index(num_entries=3, num_buckets=3)
        self.write_entry(H2(0), 1, 2, 3)
        self.write_entry(H2(3), 5, 6, 7)
        self.write_entry(H2(4), 8, 9, 10)
        assert compact_index == self.index_data.getvalue()

    def test_last_used(self):
        self.index(num_entries=3, num_buckets=6)
        self.write_deleted(H2(1))
        self.write_entry(H2(0), 1, 2, 3)
        self.write_empty(H2(2))
        self.write_entry(H2(3), 5, 6, 7)
        self.write_empty(H2(5))
        self.write_entry(H2(4), 8, 9, 10)

        compact_index = self.index_from_data_compact_to_data()

        self.index(num_entries=3, num_buckets=3)
        self.write_entry(H2(0), 1, 2, 3)
        self.write_entry(H2(3), 5, 6, 7)
        self.write_entry(H2(4), 8, 9, 10)
        assert compact_index == self.index_data.getvalue()

    def test_too_few_empty_slots(self):
        self.index(num_entries=3, num_buckets=6)
        self.write_deleted(H2(1))
        self.write_entry(H2(0), 1, 2, 3)
        self.write_entry(H2(3), 5, 6, 7)
        self.write_empty(H2(2))
        self.write_empty(H2(5))
        self.write_entry(H2(4), 8, 9, 10)

        compact_index = self.index_from_data_compact_to_data()

        self.index(num_entries=3, num_buckets=3)
        self.write_entry(H2(0), 1, 2, 3)
        self.write_entry(H2(3), 5, 6, 7)
        self.write_entry(H2(4), 8, 9, 10)
        assert compact_index == self.index_data.getvalue()

    def test_empty(self):
        self.index(num_entries=0, num_buckets=6)
        self.write_deleted(H2(1))
        self.write_empty(H2(0))
        self.write_deleted(H2(3))
        self.write_empty(H2(2))
        self.write_empty(H2(5))
        self.write_deleted(H2(4))

        compact_index = self.index_from_data_compact_to_data()

        self.index(num_entries=0, num_buckets=0)
        assert compact_index == self.index_data.getvalue()

    def test_merge(self):
        master = ChunkIndex()
        idx1 = ChunkIndex()
        idx1[H(1)] = 1, 100, 100
        idx1[H(2)] = 2, 200, 200
        idx1[H(3)] = 3, 300, 300
        idx1.compact()
        assert idx1.size() == 18 + 3 * (32 + 3 * 4)

        master.merge(idx1)
        assert master[H(1)] == (1, 100, 100)
        assert master[H(2)] == (2, 200, 200)
        assert master[H(3)] == (3, 300, 300)


class NSIndexTestCase(BaseTestCase):
    def test_nsindex_segment_limit(self):
        idx = NSIndex()
        with self.assert_raises(AssertionError):
            idx[H(1)] = NSIndex.MAX_VALUE + 1, 0
        assert H(1) not in idx
        idx[H(2)] = NSIndex.MAX_VALUE, 0
        assert H(2) in idx


class AllIndexTestCase(BaseTestCase):
    def test_max_load_factor(self):
        assert NSIndex.MAX_LOAD_FACTOR < 1.0
        assert ChunkIndex.MAX_LOAD_FACTOR < 1.0
