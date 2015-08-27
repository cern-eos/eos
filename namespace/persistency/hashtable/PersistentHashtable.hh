#ifndef HAVE_PERSISTENTHASHTABLE_HH
#define HAVE_PERSISTENTHASHTABLE_HH

#include <fstream>
#include <stack>
#include <string>
#include <memory>

#include "persistent_hashtable.h"
#include "alloc.h"

template<typename Key, typename Value>
class PersistentHashtable
{
private:
    void updateFingerprint(
        persistent_hashtable_entry_t* entry,
        unsigned long& crc
    );

public:
    persistent_hashtable_t persistent * instance;

    PersistentHashtable(size_t buckets, hash_func_t hash, comp_func_t comp);

    ~PersistentHashtable();

    void persistent * operator new(size_t size);
    void operator delete(void persistent * ptr);

    void writeFingerprint(std::ofstream& output);

    class iterator
    {
        typedef std::pair<Key&, Value&> pair;

    private:
        std::stack<persistent_hashtable_entry_t persistent *> stack;
        persistent_hashtable_t persistent * instance;

    public:
        size_t bucket;
        persistent_hashtable_entry_t persistent * current;

        iterator();
        iterator(persistent_hashtable_t persistent * inst);
        iterator(persistent_hashtable_entry_t persistent * cur);

        iterator& operator++();

        bool operator==(iterator other) const;
        bool operator!=(iterator other) const;
        std::unique_ptr<pair> operator->() const;
        pair operator*() const;
    };

    // TODO: Find a way to implement this properly without duplicating code.
    typedef iterator const_iterator;

    void insert(std::pair<Key, Value> pair);

    void erase(const Key& key);
    void erase(iterator& it);
    void clear();

    Value& operator[](const Key& index) const;
    iterator find(Key key) const;
    iterator begin() const;
    iterator end() const;

    size_t size() const;
    size_t count(Key key) const;

    void writeFingerprint(std::ofstream& output) const;
};

#include "PersistentHashtable.cc"

#endif

// vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
