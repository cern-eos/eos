#include <zlib.h>
#include <fstream>
#include <memory>
#include <stdexcept>

#include "PersistentHashtable.hh"
#include "alloc.h"
#include "util.h"

template<typename Key, typename Value>
PersistentHashtable<Key, Value>::PersistentHashtable(
    size_t entries, hash_func_t hash, comp_func_t comp)
{
    instance = hashtable_new(entries, hash, comp);
}

template<typename Key, typename Value>
PersistentHashtable<Key, Value>::~PersistentHashtable()
{
    for(auto it = begin(); it != end(); ++it) {
        persistent_hashtable_entry_t * entry = it.current;
        Key* key = (Key*) entry->key;
        Value* value = (Value*) entry->value;

        key->~Key();
        pfree(key);

        value->~Value();
        pfree(value);
    }
    hashtable_release(instance);
}

template<typename Key, typename Value>
void persistent * PersistentHashtable<Key, Value>::
    operator new(size_t size)
{
    return pmalloc(size);
}

template<typename Key, typename Value>
void PersistentHashtable<Key, Value>::
    operator delete(void persistent * ptr)
{
    return pfree(ptr);
}

template<typename Key, typename Value>
void PersistentHashtable<Key, Value>::
    updateFingerprint(persistent_hashtable_entry_t persistent * entry, unsigned long& crc)
{
    if(!entry)
        return;

    crc = crc32(crc, (const Bytef*) entry, sizeof(persistent_hashtable_entry_t));
    updateFingerprint(entry->left, crc);
    updateFingerprint(entry->right, crc);
}

template<typename Key, typename Value>
void PersistentHashtable<Key, Value>::
    writeFingerprint(std::ofstream& output)
{
    unsigned long crc;
    crc = crc32(0, NULL, 0);

    for(size_t i = 0; i < instance->bucket_count; i++) {
        updateFingerprint(instance->buckets[i], crc);
    }

    output.write((char*) &crc, 4);
}

template<typename Key, typename Value>
size_t PersistentHashtable<Key, Value>::size() const
{
    return instance->count;
}

template <typename Key, typename Value>
void PersistentHashtable<Key, Value>::insert(
    std::pair<Key, Value> pair)
{
    (*this)[pair.first] = pair.second;
}

template <typename Key, typename Value>
Value& PersistentHashtable<Key, Value>::operator[] (const Key& key) const
{
    // Because of the way C++ expects maps to work, this is the lowest
    // granularity we can have for our atomic block now. The hashtable_get
    // function will create an entry if one does not exist for this key.
    // Afterwards, we need to decide if it is a new entry and copy the key
    // and value, because we do not want to copy them needlessly.
    ATOMIC {
        persistent_hashtable_entry_t persistent* entry =
            hashtable_get(instance, &key);

        if(!entry->key) {
            // Entry did not already exist. Use placement new to ensure that
            // the memory is allocated persistently.
            void* buf_key = pmalloc(sizeof(Key));
            entry->key = new (buf_key) Key(key);

            void* buf_value = pmalloc(sizeof(Value));
            entry->value = new (buf_value) Value();
        }

        return *(Value*) entry->value;
    }
}

template <typename Key, typename Value>
size_t PersistentHashtable<Key, Value>::count(Key key) const
{
    return hashtable_get(instance, &key) == NULL;
}

template <typename Key, typename Value>
typename PersistentHashtable<Key, Value>::iterator
PersistentHashtable<Key, Value>::find(Key key) const
{
    persistent_hashtable_entry_t persistent * entry = hashtable_find(instance, &key);
    return entry ? PersistentHashtable<Key, Value>::iterator(entry) : end();
}

template<typename Key, typename Value>
void PersistentHashtable<Key, Value>::
    erase(const Key& key)
{
    const Key* key_ref = &key;
    const Value* value_ref = NULL;

    hashtable_remove(
        instance,
        (const void**) &key_ref,
        (const void**) &value_ref
    );

    if(value_ref != NULL) {
        // We call the destructor explicitly and free the buffer afterwards,
        // because these were allocated using placement new (refer to the
        // comment in operator[]).
        key_ref->~Key();
        pfree((void*) key_ref);

        value_ref->~Value();
        pfree((void*) value_ref);
    }
}

template<typename Key, typename Value>
void PersistentHashtable<Key, Value>::
    erase(PersistentHashtable<Key, Value>::iterator& it)
{
    erase(it->first);
}

template<typename Key, typename Value>
typename PersistentHashtable<Key, Value>::iterator
PersistentHashtable<Key, Value>::begin() const
{
    return PersistentHashtable<Key, Value>::iterator(instance);
}

template<typename Key, typename Value>
typename PersistentHashtable<Key, Value>::iterator
PersistentHashtable<Key, Value>::end() const
{
    return PersistentHashtable<Key, Value>::iterator();
}

template<typename Key, typename Value>
void PersistentHashtable<Key, Value>::clear()
{
    hashtable_clear(instance);
}

// Iterator methods

template<typename Key, typename Value>
PersistentHashtable<Key, Value>::iterator::
    iterator(persistent_hashtable_t persistent * inst)
{
    instance = inst;
    bucket = 0;

    while(bucket < instance->bucket_count &&
          !instance->buckets[bucket]) {
        bucket++;
    }

    if(bucket < instance->bucket_count) {
        current = instance->buckets[bucket];
    } else {
        current = NULL;
    }
}

template<typename Key, typename Value>
PersistentHashtable<Key, Value>::iterator::
    iterator(persistent_hashtable_entry_t persistent * cur)
{
    instance = NULL;
    current = cur;
    bucket = 0;
}

template<typename Key, typename Value>
PersistentHashtable<Key, Value>::iterator::iterator()
{
    instance = NULL;
    bucket = 0;
    current = NULL;
}

template<typename Key, typename Value>
typename PersistentHashtable<Key, Value>::iterator&
PersistentHashtable<Key, Value>::iterator::operator++()
{
    if(!current) {
        return *this;
    }

    if(current->left) {
        // Node has two children. We keep the right subtree
        // for later exploration.
        if(current->right) {
            stack.push(current->right);
        }
        current = current->left;
    } else if(current->right) {
        current = current->right;
    } else if(stack.size()) {
        // We hit the bottom of the tree, find a right subtree
        // that is yet to be explored.
        current = stack.top();
        stack.pop();
    } else {
        // This tree is done, find the next one to explore
        do {
            bucket++;
        } while(bucket < instance->bucket_count &&
                !instance->buckets[bucket]);

        if(bucket < instance->bucket_count) {
            current = instance->buckets[bucket];
        } else {
            current = NULL;
        }
    }

    return *this;
}

template<typename Key, typename Value>
bool PersistentHashtable<Key, Value>::iterator::operator==(iterator other)
    const
{
    return current == other.current;
}

template<typename Key, typename Value>
bool PersistentHashtable<Key, Value>::iterator::operator!=(iterator other)
    const
{
    return !(*this == other);
}

template<typename Key, typename Value>
typename PersistentHashtable<Key, Value>::iterator::pair
PersistentHashtable<Key, Value>::iterator::operator*() const
{
    return std::pair<Key&, Value&>(
        *(Key*) current->key,
        *(Value*) current->value
    );
}

template<typename Key, typename Value>
std::unique_ptr<typename PersistentHashtable<Key, Value>::iterator::pair>
    PersistentHashtable<Key, Value>::iterator::operator->() const
{
    // This overload requires that a pointer or an object overloading
    // 'operator->' is returned. To avoid return the address of a temporary
    // while still not leaking memory we use this approach from C++11.
    return std::unique_ptr<std::pair<Key&, Value&>>(
        new std::pair<Key&, Value&>(
            *(Key*) current->key,
            *(Value*) current->value
        )
    );
}

// Instantiate templates to allow linking against the object file.
template class PersistentHashtable<std::string, std::string>;
template class PersistentHashtable<long long, std::string>;

// vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
