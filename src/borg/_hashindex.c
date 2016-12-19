#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#if defined (__SVR4) && defined (__sun)
#include <sys/isa_defs.h>
#endif

#if (defined(BYTE_ORDER)&&(BYTE_ORDER == BIG_ENDIAN)) ||  \
    (defined(_BIG_ENDIAN)&&defined(__SVR4)&&defined(__sun))
#define _le32toh(x) __builtin_bswap32(x)
#define _htole32(x) __builtin_bswap32(x)
#elif (defined(BYTE_ORDER)&&(BYTE_ORDER == LITTLE_ENDIAN)) || \
      (defined(_LITTLE_ENDIAN)&&defined(__SVR4)&&defined(__sun))
#define _le32toh(x) (x)
#define _htole32(x) (x)
#else
#error Unknown byte order
#endif

#define MAGIC "BORG_IDX"
#define MAGIC_LEN 8

typedef struct {
    char magic[MAGIC_LEN];
    int32_t num_entries;
    int32_t num_buckets;
    int8_t  key_size;
    int8_t  value_size;
} __attribute__((__packed__)) HashHeader;

typedef struct {
    void *buckets;
    int num_entries;
    int num_buckets;
    int key_size;
    int value_size;
    off_t bucket_size;
    int lower_limit;
    int upper_limit;
} HashIndex;

/* prime (or w/ big prime factors) hash table sizes
 * not sure we need primes for borg's usage (as we have a hash function based
 * on sha256, we can assume an even, seemingly random distribution of values),
 * but OTOH primes don't harm.
 * also, growth of the sizes starts with fast-growing 2x steps, but slows down
 * more and more down to 1.1x. this is to avoid huge jumps in memory allocation,
 * like e.g. 4G -> 8G.
 * these values are generated by hash_sizes.py.
 */
static int hash_sizes[] = {
    1031, 2053, 4099, 8209, 16411, 32771, 65537, 131101, 262147, 445649,
    757607, 1287917, 2189459, 3065243, 4291319, 6007867, 8410991,
    11775359, 16485527, 23079703, 27695653, 33234787, 39881729, 47858071,
    57429683, 68915617, 82698751, 99238507, 119086189, 144378011, 157223263,
    173476439, 190253911, 209915011, 230493629, 253169431, 278728861,
    306647623, 337318939, 370742809, 408229973, 449387209, 493428073,
    543105119, 596976533, 657794869, 722676499, 795815791, 874066969,
    962279771, 1057701643, 1164002657, 1280003147, 1407800297, 1548442699,
    1703765389, 1873768367, 2062383853, /* 32bit int ends about here */
};

#define HASH_MIN_LOAD .25
#define HASH_MAX_LOAD .93  /* use testsuite.benchmark.test_chunk_indexer_* to find
                              an appropriate value; also don't forget to update this
                              value in archive.py */

#define MAX(x, y) ((x) > (y) ? (x): (y))
#define NELEMS(x) (sizeof(x) / sizeof((x)[0]))

#define EMPTY _htole32(0xffffffff)
#define DELETED _htole32(0xfffffffe)

#define BUCKET_ADDR(index, idx) (index->buckets + (idx * index->bucket_size))

#define BUCKET_MATCHES_KEY(index, idx, key) (memcmp(key, BUCKET_ADDR(index, idx), index->key_size) == 0)

#define BUCKET_IS_DELETED(index, idx) (*((uint32_t *)(BUCKET_ADDR(index, idx) + index->key_size)) == DELETED)
#define BUCKET_IS_EMPTY(index, idx) (*((uint32_t *)(BUCKET_ADDR(index, idx) + index->key_size)) == EMPTY)

#define BUCKET_MARK_DELETED(index, idx) (*((uint32_t *)(BUCKET_ADDR(index, idx) + index->key_size)) = DELETED)
#define BUCKET_MARK_EMPTY(index, idx) (*((uint32_t *)(BUCKET_ADDR(index, idx) + index->key_size)) = EMPTY)

#define EPRINTF_MSG(msg, ...) fprintf(stderr, "hashindex: " msg "\n", ##__VA_ARGS__)
#define EPRINTF_MSG_PATH(path, msg, ...) fprintf(stderr, "hashindex: %s: " msg "\n", path, ##__VA_ARGS__)
#define EPRINTF(msg, ...) fprintf(stderr, "hashindex: " msg "(%s)\n", ##__VA_ARGS__, strerror(errno))
#define EPRINTF_PATH(path, msg, ...) fprintf(stderr, "hashindex: %s: " msg " (%s)\n", path, ##__VA_ARGS__, strerror(errno))

static HashIndex *hashindex_read(const char *path);
static int hashindex_write(HashIndex *index, const char *path);
static HashIndex *hashindex_init(int capacity, int key_size, int value_size);
static const void *hashindex_get(HashIndex *index, const void *key);
static int hashindex_set(HashIndex *index, const void *key, const void *value);
static int hashindex_delete(HashIndex *index, const void *key);
static void *hashindex_next_key(HashIndex *index, const void *key);

/* Private API */
static void hashindex_free(HashIndex *index);

static int
hashindex_index(HashIndex *index, const void *key)
{
    return _le32toh(*((uint32_t *)key)) % index->num_buckets;
}

inline int
distance(int num_buckets, int current_idx, int ideal_idx)
{
    /* If the current index is smaller than the ideal index we've wrapped
       around the end of the bucket array and need to compensate for that. */
    return current_idx - ideal_idx + ( (current_idx < ideal_idx) ? num_buckets : 0 );
}


static int
hashindex_lookup(HashIndex *index, const void *key, int *skip_hint)
{
    int didx = -1;  // deleted index
    int start = hashindex_index(index, key);
    int idx = start;
    int num_buckets = index->num_buckets;
    int offset;
    for(offset=0; ; offset++) {
        if (skip_hint != NULL) {
            (*skip_hint) = offset;
        }
        if(BUCKET_IS_EMPTY(index, idx))
        {
            return -1;
        }
        if(offset > distance(num_buckets, idx, start)) {
            return -1;
        }
        if(BUCKET_IS_DELETED(index, idx)) {
            /* TODO: don't hint at skipping deleted buckets */
            if(didx == -1) {
                didx = idx;
            }
        }
        else if(BUCKET_MATCHES_KEY(index, idx, key)) {
            if (didx != -1) {
                /* we found a tombstone earlier, so we can move this key on top of it */
                memcpy(BUCKET_ADDR(index, didx), BUCKET_ADDR(index, idx), index->bucket_size);
                BUCKET_MARK_DELETED(index, idx);
                idx = didx;
            }
            return idx;
        }
        idx = (idx + 1) % index->num_buckets;
        if(idx == start) {
            return -1;
        }
    }
}

static int
hashindex_resize(HashIndex *index, int capacity)
{
    HashIndex *new;
    void *key = NULL;
    int32_t key_size = index->key_size;

    if(!(new = hashindex_init(capacity, key_size, index->value_size))) {
        return 0;
    }
    while((key = hashindex_next_key(index, key))) {
        if(!hashindex_set(new, key, key + key_size)) {
            /* This can only happen if there's a bug in the code calculating capacity */
            hashindex_free(new);
            return 0;
        }
    }
    free(index->buckets);
    index->buckets = new->buckets;
    index->num_buckets = new->num_buckets;
    index->lower_limit = new->lower_limit;
    index->upper_limit = new->upper_limit;
    free(new);
    return 1;
}

int get_lower_limit(int num_buckets){
    int min_buckets = hash_sizes[0];
    if (num_buckets <= min_buckets)
        return 0;
    return (int)(num_buckets * HASH_MIN_LOAD);
}

int get_upper_limit(int num_buckets){
    int max_buckets = hash_sizes[NELEMS(hash_sizes) - 1];
    if (num_buckets >= max_buckets)
        return num_buckets;
    return (int)(num_buckets * HASH_MAX_LOAD);
}

int size_idx(int size){
    /* find the hash_sizes index with entry >= size */
    int elems = NELEMS(hash_sizes);
    int entry, i=0;
    do{
        entry = hash_sizes[i++];
    }while((entry < size) && (i < elems));
    if (i >= elems)
        return elems - 1;
    i--;
    return i;
}

int fit_size(int current){
    int i = size_idx(current);
    return hash_sizes[i];
}

int grow_size(int current){
    int i = size_idx(current) + 1;
    int elems = NELEMS(hash_sizes);
    if (i >= elems)
        return hash_sizes[elems - 1];
    return hash_sizes[i];
}

int shrink_size(int current){
    int i = size_idx(current) - 1;
    if (i < 0)
        return hash_sizes[0];
    return hash_sizes[i];
}

/* Public API */
static HashIndex *
hashindex_read(const char *path)
{
    FILE *fd;
    off_t length, buckets_length, bytes_read;
    HashHeader header;
    HashIndex *index = NULL;

    if((fd = fopen(path, "rb")) == NULL) {
        EPRINTF_PATH(path, "fopen for reading failed");
        return NULL;
    }
    bytes_read = fread(&header, 1, sizeof(HashHeader), fd);
    if(bytes_read != sizeof(HashHeader)) {
        if(ferror(fd)) {
            EPRINTF_PATH(path, "fread header failed (expected %ju, got %ju)",
                         (uintmax_t) sizeof(HashHeader), (uintmax_t) bytes_read);
        }
        else {
            EPRINTF_MSG_PATH(path, "fread header failed (expected %ju, got %ju)",
                             (uintmax_t) sizeof(HashHeader), (uintmax_t) bytes_read);
        }
        goto fail;
    }
    if(fseek(fd, 0, SEEK_END) < 0) {
        EPRINTF_PATH(path, "fseek failed");
        goto fail;
    }
    if((length = ftell(fd)) < 0) {
        EPRINTF_PATH(path, "ftell failed");
        goto fail;
    }
    if(fseek(fd, sizeof(HashHeader), SEEK_SET) < 0) {
        EPRINTF_PATH(path, "fseek failed");
        goto fail;
    }
    if(memcmp(header.magic, MAGIC, MAGIC_LEN)) {
        EPRINTF_MSG_PATH(path, "Unknown MAGIC in header");
        goto fail;
    }
    buckets_length = (off_t)_le32toh(header.num_buckets) * (header.key_size + header.value_size);
    if((size_t) length != sizeof(HashHeader) + buckets_length) {
        EPRINTF_MSG_PATH(path, "Incorrect file length (expected %ju, got %ju)",
                         (uintmax_t) sizeof(HashHeader) + buckets_length, (uintmax_t) length);
        goto fail;
    }
    if(!(index = malloc(sizeof(HashIndex)))) {
        EPRINTF_PATH(path, "malloc header failed");
        goto fail;
    }
    if(!(index->buckets = malloc(buckets_length))) {
        EPRINTF_PATH(path, "malloc buckets failed");
        free(index);
        index = NULL;
        goto fail;
    }
    bytes_read = fread(index->buckets, 1, buckets_length, fd);
    if(bytes_read != buckets_length) {
        if(ferror(fd)) {
            EPRINTF_PATH(path, "fread buckets failed (expected %ju, got %ju)",
                         (uintmax_t) buckets_length, (uintmax_t) bytes_read);
        }
        else {
            EPRINTF_MSG_PATH(path, "fread buckets failed (expected %ju, got %ju)",
                             (uintmax_t) buckets_length, (uintmax_t) bytes_read);
        }
        free(index->buckets);
        free(index);
        index = NULL;
        goto fail;
    }
    index->num_entries = _le32toh(header.num_entries);
    index->num_buckets = _le32toh(header.num_buckets);
    index->key_size = header.key_size;
    index->value_size = header.value_size;
    index->bucket_size = index->key_size + index->value_size;
    index->lower_limit = get_lower_limit(index->num_buckets);
    index->upper_limit = get_upper_limit(index->num_buckets);
fail:
    if(fclose(fd) < 0) {
        EPRINTF_PATH(path, "fclose failed");
    }
    return index;
}

static HashIndex *
hashindex_init(int capacity, int key_size, int value_size)
{
    HashIndex *index;
    int i;
    capacity = fit_size(capacity);

    if(!(index = malloc(sizeof(HashIndex)))) {
        EPRINTF("malloc header failed");
        return NULL;
    }
    if(!(index->buckets = calloc(capacity, key_size + value_size))) {
        EPRINTF("malloc buckets failed");
        free(index);
        return NULL;
    }
    index->num_entries = 0;
    index->key_size = key_size;
    index->value_size = value_size;
    index->num_buckets = capacity;
    index->bucket_size = index->key_size + index->value_size;
    index->lower_limit = get_lower_limit(index->num_buckets);
    index->upper_limit = get_upper_limit(index->num_buckets);
    for(i = 0; i < capacity; i++) {
        BUCKET_MARK_EMPTY(index, i);
    }
    return index;
}

static void
hashindex_free(HashIndex *index)
{
    free(index->buckets);
    free(index);
}

static int
hashindex_write(HashIndex *index, const char *path)
{
    off_t buckets_length = (off_t)index->num_buckets * index->bucket_size;
    FILE *fd;
    HashHeader header = {
        .magic = MAGIC,
        .num_entries = _htole32(index->num_entries),
        .num_buckets = _htole32(index->num_buckets),
        .key_size = index->key_size,
        .value_size = index->value_size
    };
    int ret = 1;

    if((fd = fopen(path, "wb")) == NULL) {
        EPRINTF_PATH(path, "fopen for writing failed");
        return 0;
    }
    if(fwrite(&header, 1, sizeof(header), fd) != sizeof(header)) {
        EPRINTF_PATH(path, "fwrite header failed");
        ret = 0;
    }
    if(fwrite(index->buckets, 1, buckets_length, fd) != (size_t) buckets_length) {
        EPRINTF_PATH(path, "fwrite buckets failed");
        ret = 0;
    }
    if(fclose(fd) < 0) {
        EPRINTF_PATH(path, "fclose failed");
    }
    return ret;
}

static const void *
hashindex_get(HashIndex *index, const void *key)
{
    int idx = hashindex_lookup(index, key, NULL);
    if(idx < 0) {
        return NULL;
    }
    return BUCKET_ADDR(index, idx) + index->key_size;
}

inline void
memswap(void *a, void *b, void *tmp, size_t entry_size) {
    memcpy(tmp, a, entry_size);
    memcpy(a, b, entry_size);
    memcpy(b, tmp, entry_size);
}

static int
hashindex_set(HashIndex *index, const void *key, const void *value)
{
    int offset = 0;
    int idx = hashindex_lookup(index, key, &offset);
    uint8_t *bucket_ptr;
    int other_offset;
    int entry_size = (index->key_size + index->value_size);
    static void *entry_to_insert = NULL;
    static void *tmp_entry = NULL;
    int num_buckets = index->num_buckets;
    if (entry_to_insert == NULL) {
        entry_to_insert = malloc(entry_size * 2);
        tmp_entry = entry_to_insert + entry_size;
    }
    if(idx < 0)
    {
        /* we don't have the key in the index we need to find an appropriate address */
        if(index->num_entries > index->upper_limit) {
            /* we need to grow the hashindex */
            if(!hashindex_resize(index, grow_size(index->num_buckets))) {
                return 0;
            }
            offset = 0;
        }
        idx = (hashindex_index(index, key) + offset) % index->num_buckets;
        memcpy(entry_to_insert, key, index->key_size);
        memcpy(entry_to_insert + index->key_size, value, index->value_size);
        bucket_ptr = BUCKET_ADDR(index, idx);
        while(!BUCKET_IS_EMPTY(index, idx) && !BUCKET_IS_DELETED(index, idx)) {
            /* we have a collision */
            other_offset = distance(
                num_buckets, idx, hashindex_index(index, bucket_ptr));
            if(other_offset < offset) {
                /* Swap the bucket at idx with the current entry_to_insert.
                   This is the gist of robin-hood hashing, we rob from the key with the
                   lower distance to its optimal address by swapping places with it. */
                memswap(bucket_ptr, entry_to_insert, tmp_entry, entry_size);
                offset = other_offset;
            }
            offset++;
            idx = (idx + 1) % index->num_buckets;
            bucket_ptr = BUCKET_ADDR(index, idx);
        }
        memcpy(bucket_ptr, entry_to_insert, entry_size);
        index->num_entries += 1;
    }
    else
    {
        /* we already have the key in the index we just need to update its value */
        memcpy(BUCKET_ADDR(index, idx) + index->key_size, value, index->value_size);
    }
    return 1;
}

static int
hashindex_delete(HashIndex *index, const void *key)
{
    int idx = hashindex_lookup(index, key, NULL);
    if (idx < 0) {
        return 1;
    }
    BUCKET_MARK_DELETED(index, idx);
    index->num_entries -= 1;
    if(index->num_entries < index->lower_limit) {
        if(!hashindex_resize(index, shrink_size(index->num_buckets))) {
            return 0;
        }
    }
    return 1;
}

static void *
hashindex_next_key(HashIndex *index, const void *key)
{
    int idx = 0;
    if(key) {
        idx = 1 + (key - index->buckets) / index->bucket_size;
    }
    if (idx == index->num_buckets) {
        return NULL;
    }
    while(BUCKET_IS_EMPTY(index, idx) || BUCKET_IS_DELETED(index, idx)) {
        idx ++;
        if (idx == index->num_buckets) {
            return NULL;
        }
    }
    return BUCKET_ADDR(index, idx);
}

static int
hashindex_len(HashIndex *index)
{
    return index->num_entries;
}

static int
hashindex_size(HashIndex *index)
{
    return sizeof(HashHeader) + index->num_buckets * index->bucket_size;
}

static void
benchmark_getitem(HashIndex *index, char *keys, int key_count)
{
  char *key = keys;
  unsigned long hits = 0;
  unsigned long misses = 0;
  char *last_addr = key + (32 * key_count);
  while (key < last_addr) {
    if (hashindex_get(index, key) != NULL) {
      hits += 1;
    } else {
      misses += 1;
    }
    key += 32;
  }
  /* printf("hits %lud\nmisses %lud\n", hits, misses); */
}
