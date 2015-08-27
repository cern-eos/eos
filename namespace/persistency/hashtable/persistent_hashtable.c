/* persistent_hashtable.c
 *
 * Extremely simple hash table implementation. Splits values into a number of
 * buckets based on the hash value given to them by the hash function
 * provided. Collisions are handled by making each bucket a binary tree.
 */

#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "alloc.h"
#include "persistent_hashtable.h"
#include "util.h"

// Initializes hashtable parameters.
persistent_hashtable_t persistent * hashtable_new(
    size_t bucket_count,
    hash_func_t hash_func,
    comp_func_t comp_func
)
{
    size_t hash_size = sizeof(persistent_hashtable_t) + \
        sizeof(persistent_hashtable_entry_t persistent *) * bucket_count;

    persistent_hashtable_t persistent * hash =
        (persistent_hashtable_t persistent *) pmalloc(hash_size);

    hash->hash_func = hash_func;
    hash->comp_func = comp_func;
    hash->bucket_count = bucket_count;
    hash->count = 0;

    // Initialize all buckets to empty.
    unsigned i;
    for(i = 0; i < bucket_count; i++) {
        hash->buckets[i] = NULL;
    }

    return hash;
}

static void hashtable_rotate_left(persistent_hashtable_entry_t persistent ** root)
{
    persistent_hashtable_entry_t persistent * parent = *root;

    if(!parent->right)
        return;

    ATOMIC {
        persistent_hashtable_entry_t persistent * pivot = parent->right;
        *root = pivot;

        parent->right = pivot->left;
        pivot->left = parent;

        // Update the balances. This can be done without explicitly
        // recalculating the heights of the subtrees, by carefully working out
        // the equations for the new balances from the old ones.
        parent->balance++;
        if(pivot->balance < 0) {
            parent->balance -= pivot->balance;
        }

        pivot->balance++;
        if(parent->balance > 0) {
            pivot->balance += parent->balance;
        }
    }
}

static void hashtable_rotate_right(persistent_hashtable_entry_t persistent ** root)
{
    persistent_hashtable_entry_t persistent * parent = *root;

    if(!parent->left)
        return;

    ATOMIC {
        persistent_hashtable_entry_t persistent * pivot = parent->left;
        *root = pivot;

        parent->left = pivot->right;
        pivot->right = parent;

        // Symmetric to left rotation
        parent->balance--;
        if(pivot->balance > 0) {
            parent->balance -= pivot->balance;
        }

        pivot->balance--;
        if(parent->balance < 0) {
            pivot->balance += parent->balance;
        }
    }
}

// Rebalances a node when the balance factor increases after insertion.
static bool hashtable_rebalance_increase_insert(
    persistent_hashtable_entry_t persistent ** root
)
{
    persistent_hashtable_entry_t persistent * parent = *root;

    // Increase the balance with respect to the original balance. This erases
    // the 'dirty' marking we gave earlier; refer to the comment in
    // hashtable_insert_into for further details.
    parent->balance++;

    if(parent->balance == 2) {
        if(parent->left->balance == -1) {
            // We have a left-right case, reduce it to right-right case.
            hashtable_rotate_left(&parent->left);
        }
        // Guaranteed right-right case, rotate to balanced tree.
        hashtable_rotate_right(root);

        // The fact that we ended up with a balance factor of two after
        // insertion meant that the tree was slightly out of balance. After
        // rotation, we have a balance factor of 0. This means that the
        // height of this subtree did not change. We can stop the rotation
        // from this point on.
        return false;
    }

    // If the balance factor is one after insertion, it means one of the two
    // subtrees increased in height, exceeding the height of the other subtree
    // (since the balance factor used to be 0). We need to proceed rebalancing up
    // the tree.
    return (*root)->balance == 1;
}

// Rebalances a node when the balance factor decreases after insertion.
// This method is symmetric to hashtable_rebalance_increase_insert above.
static bool hashtable_rebalance_decrease_insert(
    persistent_hashtable_entry_t persistent ** root
)
{
    persistent_hashtable_entry_t persistent* parent = *root;

    parent->balance--;

    if(parent->balance == -2) {
        if(parent->right->balance == 1) {
            hashtable_rotate_right(&parent->right);
        }
        hashtable_rotate_left(root);
        return false;
    }

    return (*root)->balance == -1;
}

// Rebalances a node after the balance factor increases after deletion.
static bool hashtable_rebalance_increase_delete(
    persistent_hashtable_entry_t persistent ** root,
    char balance
)
{
    persistent_hashtable_entry_t persistent * parent = *root;

    // The balance factor should increase after deletion if we delete from the
    // right subtree. Apply this change ahead of time and then figure out the
    // correct rotation.
    parent->balance = balance + 1;

    if(parent->balance == 2) {
        // We are out of balance, rotation is needed.
        char sub_balance = parent->left->balance;

        if(sub_balance == -1) {
            // Left right case, reduce to left left case
            hashtable_rotate_left(&parent->left);
        }
        hashtable_rotate_right(root);

        if(sub_balance == 0) {
            // Left child had a balance factor of zero. This means the subtree
            // height did not change and we can stop rebalancing.
            return false;
        }
    } else if(parent->balance == 1) {
        // The tree used to be balanced. The change in height is 'absorbed' by
        // the node, because the other subtree makes sure the height stays the
        // same.
        return false;
    }

    return true;
}

// Rebalances a node after when balance factor decreases after deletion.
// This method is analogous to hashtable_rebalance_increase_delete above.
static bool hashtable_rebalance_decrease_delete(
    persistent_hashtable_entry_t persistent ** root,
    char balance
)
{
    persistent_hashtable_entry_t persistent * parent = *root;

    parent->balance = balance - 1;

    if(parent->balance == -2) {
        char sub_balance = parent->right->balance;

        if(sub_balance == 1) {
            hashtable_rotate_right(&parent->right);
        }
        hashtable_rotate_left(root);

        if(sub_balance == 0) {
            return false;
        }
    } else if(parent->balance == -1) {
        return false;
    }

    return true;
}

// Inserts the key and value into the tree rooted at root. Note that a double
// pointer is passed to fill up the reference from the parent if need be.
static bool hashtable_get_from(
    persistent_hashtable_t persistent * hash,
    persistent_hashtable_entry_t persistent ** root,
    persistent_hashtable_entry_t persistent ** found,
    const void persistent * key
)
{
    persistent_hashtable_entry_t persistent * parent = *root;

    if(!parent) {
        // We found an empty place to insert. Create an entry and fill it.
        persistent_hashtable_entry_t persistent * entry =
            pmalloc(sizeof(persistent_hashtable_entry_t));
        memset(entry, 0, sizeof(persistent_hashtable_entry_t));

        *found = entry;
        *root = entry;
        hash->count++;

        return true;
    }

    int cmp = hash->comp_func(key, parent->key);
    if(cmp == 0) {
        // An entry with this key was found, overwrite the value lazily.
        *found = parent;
        return false;
    }

    if(cmp < 0) {
        if(!hashtable_get_from(hash, &parent->left, found, key)) {
            // No change in weight, signal this up the recursion
            return false;
        } else {
            return hashtable_rebalance_increase_insert(root);
        }
    } else {
        // Analogous to the above
        if(!hashtable_get_from(hash, &parent->right, found, key)) {
            return false;
        } else {
            return hashtable_rebalance_decrease_insert(root);
        }
    }
}

// Inserts the given key and value into the hashtable.
persistent_hashtable_entry_t persistent* hashtable_get(
    persistent_hashtable_t persistent * hash,
    const void* key
)
{
    size_t bucket = hash->hash_func(key) % hash->bucket_count;
    persistent_hashtable_entry_t persistent * entry;
    hashtable_get_from(hash, &hash->buckets[bucket], &entry, key);

    return entry;
}

static persistent_hashtable_entry_t persistent* hashtable_find_in(
    persistent_hashtable_t persistent * hash,
    persistent_hashtable_entry_t persistent* current,
    const void* key
)
{
    if(!current) {
        // We hit a dead end
        return NULL;
    }

    int cmp = hash->comp_func(key, current->key);
    if(cmp == 0) {
        // Key found
        return current;
    }

    if(cmp < 0) {
        // Search the left subtree
        return hashtable_find_in(hash, current->left, key);
    } else {
        // Search the right subtree
        return hashtable_find_in(hash, current->right, key);
    }
}


persistent_hashtable_entry_t persistent* hashtable_find(
    persistent_hashtable_t persistent * hash,
    const void* key
)
{
    size_t bucket = hash->hash_func(key) % hash->bucket_count;
    return hashtable_find_in(hash, hash->buckets[bucket], key);
}

// Releases the memory allocated for the entry.
static void hashtable_release_entry(persistent_hashtable_entry_t persistent * entry)
{
    pfree(entry);
}

// Returns the address of the pointer to the leftmost child in the given tree.
static persistent_hashtable_entry_t persistent * hashtable_leftmost_child(
    persistent_hashtable_entry_t persistent * root)
{
    while(root->left) {
        root = root->left;
    }

    return root;
}

// Removes the entry pointed to by the address loc from the hashtable.
static bool hashtable_remove_from(
    persistent_hashtable_t persistent * hash,
    persistent_hashtable_entry_t persistent ** root,
    const void persistent ** key,
    const void persistent ** value
)
{
    persistent_hashtable_entry_t persistent * parent = *root;

    if(!parent) {
        // We landed in an empty tree, so apparently we do not have the key.
        return false;
    }

    int cmp = hash->comp_func(*key, parent->key);
    char balance = parent->balance;

    if(cmp < 0) {
        // Key should be in the left subtree, recurse
        if(!hashtable_remove_from(hash, &parent->left, key, value)) {
            parent->balance = balance;
            return false;
        } else {
            return hashtable_rebalance_decrease_delete(root, balance);
        }
    } else if(cmp > 0) {
        // Key should be in the right subtree, recurse.
        if(!hashtable_remove_from(hash, &parent->right, key, value)) {
            parent->balance = balance;
            return false;
        } else {
            return hashtable_rebalance_increase_delete(root, balance);
        }
    }

    // In this path, the current node is the node to be deleted. There are four
    // cases to be distinguished, depending on amount of children that the node
    // to be removed has.

    if(!parent->left && !parent->right) {
        // Easiest case: no children
        ATOMIC {
            *root = NULL;
            *key = parent->key;
            *value = parent->value;

            hashtable_release_entry(parent);
            hash->count--;
            return true;
        }
    } else if(parent->left && !parent->right) {
        // Another easy case: only one child, replace them
        ATOMIC {
            *root = parent->left;
            *key = parent->key;
            *value = parent->value;

            hashtable_release_entry(parent);
            hash->count--;
            return true;
        }
    } else if(!parent->left && parent->right) {
        // Symmetric to the previous case
        ATOMIC {
            *root = parent->right;
            *key = parent->key;
            *value = parent->value;

            hashtable_release_entry(parent);
            hash->count--;
            return true;
        }
    } else {
        // Hardest case: two children. We do a switch and then recurse
        persistent_hashtable_entry_t persistent * replace =
            hashtable_leftmost_child(parent->right);

        ATOMIC {
            // Lazy swap: just swap the key and value
            swap(&parent->key, &replace->key);
            swap(&parent->value, &replace->value);
        }

        // Mark the balance as dirty, like we do for insertions.
        parent->balance = BALANCE_DIRTY;

        // Recurse further down the tree to delete the node that we just
        // swapped. Note that we cannot simply unlink the node we found
        // above, because we need to correct weights on the path back up.
        if(!hashtable_remove_from(hash, &parent->right, key, value)) {
            parent->balance = balance;
            return false;
        } else {
            return hashtable_rebalance_increase_delete(root, balance);
        }
    }
}

// Removes the entry matching the given key from the hashtable, if it exists.
void hashtable_remove(
    persistent_hashtable_t persistent * hash,
    const void persistent ** key,
    const void persistent ** value)
{
    size_t bucket = hash->hash_func(*key) % hash->bucket_count;
    hashtable_remove_from(hash, &hash->buckets[bucket], key, value);
}

static void hashtable_clear_from(
    persistent_hashtable_entry_t persistent * entry)
{
    if(!entry)
        return;

    hashtable_clear_from(entry->left);
    hashtable_clear_from(entry->right);

    hashtable_release_entry(entry);
}

// Removes all entries from the hashtable
void hashtable_clear(persistent_hashtable_t persistent * hash)
{
    size_t i = 0;
    ATOMIC {
        for(i = 0; i < hash->bucket_count; i++) {
            hashtable_clear_from(hash->buckets[i]);
            hash->buckets[i] = NULL;
        }

        hash->count = 0;
    }
}

// Release all memory associated to the hashtable, freeing the keys and
// values.
void hashtable_release(persistent_hashtable_t persistent * hash)
{
    hashtable_clear(hash);
    pfree(hash);
}

 // vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
