import struct

from ..hashindex import ChunkIndex
from . import BaseTestCase


NUM_BUCKETS = 1031
PACK_STRING = "<I" + ("x" * 28)


def K(idx, wraps=0):
    """
    Takes an address and number of times we want it to wrap
    Useful for generating colliding keys.
    """
    key = struct.pack(PACK_STRING, idx + (NUM_BUCKETS*wraps))
    return key


def RK(key):
    """
    Reverse K.
    Returns tuple of address and number of times it wraped.
    """
    key = struct.unpack(PACK_STRING, key)[-1]
    return (key % NUM_BUCKETS, key // NUM_BUCKETS)


class RobinHood(BaseTestCase):

    def test_chunk_indexer_setitem_collision(self):
        index = ChunkIndex(NUM_BUCKETS)
        index[K(0)] = 0, 0, 0
        index[K(1)] = 1, 0, 0
        index[K(2)] = 2, 0, 0
        index[K(0, 1)] = 3, 0, 0  # this should shift the previous 2
        assert [(RK(k), v) for k, v in index.iteritems()] == [
            ((0, 0), (0, 0, 0)),
            ((0, 1), (3, 0, 0)),
            ((1, 0), (1, 0, 0)),
            ((2, 0), (2, 0, 0)),
        ]

    def test_chunk_indexer_setitem_collision_wrap(self):
        # pylint: disable=bad-whitespace
        index = ChunkIndex(NUM_BUCKETS)
        before_last = NUM_BUCKETS - 2
        index[K(0)] =                0, 0, 0  # 0
        index[K(1)] =                1, 0, 0  # 1
        index[K(before_last)] =      2, 0, 0  # 1029
        index[K(before_last + 1)] =  3, 0, 0  # 1030
        index[K(before_last, 1)] =   4, 0, 0  # this should shift the previous 1
        assert [(RK(k), v) for k, v in index.iteritems()] == [
            ((before_last+1, 0), (3, 0, 0)),  # got shifted past the end of the bucket array
            ((0, 0),             (0, 0, 0)),  # displaced from 0
            ((1, 0),             (1, 0, 0)),  # displaced from 1
            ((before_last, 0),   (2, 0, 0)),  # 1029
            ((before_last, 1),   (4, 0, 0)),  # 1030 after displacing the bucket now at 0
        ]

    def test_chunk_indexer_delete(self):
        # pylint: disable=bad-whitespace
        index = ChunkIndex(NUM_BUCKETS)
        before_last = NUM_BUCKETS - 2
        index[K(0)] =    0, 0, 0
        index[K(1)] =    1, 0, 0
        index[K(2)] =    2, 0, 0
        index[K(0, 1)] = 3, 0, 0
        assert [(RK(k), v) for k, v in index.iteritems()] == [
            ((0, 0), (0, 0, 0)),
            ((0, 1), (3, 0, 0)),
            ((1, 0), (1, 0, 0)),
            ((2, 0), (2, 0, 0)),
        ]
        del index[K(0)]
        assert [(RK(k), v) for k, v in index.iteritems()] == [
            ((0, 1), (3, 0, 0)),
            ((1, 0), (1, 0, 0)),
            ((2, 0), (2, 0, 0)),
        ]

    def test_chunk_indexer_delele_wrap(self):
        # pylint: disable=bad-whitespace
        index = ChunkIndex(NUM_BUCKETS)
        before_last = NUM_BUCKETS - 2
        index[K(0)] =                0, 0, 0  # 0
        index[K(1)] =                1, 0, 0  # 1
        index[K(before_last)] =      2, 0, 0  # 1029
        index[K(before_last + 1)] =  3, 0, 0  # 1030
        assert [(RK(k), v) for k, v in index.iteritems()] == [
            ((0, 0),             (0, 0, 0)),
            ((1, 0),             (1, 0, 0)),
            ((before_last, 0),   (2, 0, 0)),  # 1029
            ((before_last+1, 0), (3, 0, 0)),  # 1030
        ]
        import sys
        sys.stderr.write("  >>  collide\n")
        index[K(before_last, 1)] =   4, 0, 0  # this should be in 1029, collide 1030
        assert [(RK(k), v) for k, v in index.iteritems()] == [
            ((before_last+1, 0), (3, 0, 0)),  # got shifted past the end of the bucket array
            ((0, 0),             (0, 0, 0)),  # displaced from 0
            ((1, 0),             (1, 0, 0)),  # displaced from 1
            ((before_last, 0),   (2, 0, 0)),  # 1029
            ((before_last, 1),   (4, 0, 0)),  # 1030 after displacing the bucket now at 0
        ]
        from pprint import pprint
        pprint([(RK(k), v) for k, v in index.iteritems()])
        print('>>> delete', before_last)
        del index[K(before_last)]
        # this is the actual assertion of the test, the previous ones are just
        # for self-documenting and sanity checks
        assert [(RK(k), v) for k, v in index.iteritems()] == [
            ((0, 0),             (0, 0, 0)),  # back to 0
            ((1, 0),             (1, 0, 0)),  # back to 1
            ((before_last, 1),   (4, 0, 0)),  # shifted to 1029
            ((before_last+1, 0), (3, 0, 0)),  # got wrapped back to the end
        ]

    def test_chunk_indexer_setitem_collision_period(self):
        # ensure the periodical check 'distance from ideal' is doing the right thing
        index = ChunkIndex(NUM_BUCKETS)
        period = 128  # this must be >= to the period used in _hashindex.c::hashindex_lookup
        for key in range(period):
            # prepare large contiguous chunk of filled buckets
            index[K(key)] = key, 0, 0
        index[K(0, 1)] = 255, 0, 0
        expected = [
            ((0, 0), (0, 0, 0)),
            ((0, 1), (255, 0, 0)),
        ] + [
            ((key, 0), (key, 0, 0))
            for key in range(1, period)
        ]
        assert [(RK(k), v) for k, v in index.iteritems()] == expected
