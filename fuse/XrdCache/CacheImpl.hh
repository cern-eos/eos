//------------------------------------------------------------------------------
#ifndef __EOS_CACHEIMPL_HH__
#define __EOS_CACHEIMPL_HH__
//------------------------------------------------------------------------------
#include <time.h>
#include <list>
//------------------------------------------------------------------------------
#include "XrdPosix/XrdPosixXrootd.hh"
//------------------------------------------------------------------------------

template <
  typename K,
  typename V,
  typename F,
  typename M,
  template<typename...> class MAP
  >
class CacheImpl
{
 public:
  typedef K  key_type;
  typedef V  value_type;
  typedef F  file_abst;
  typedef M  mgm_cache;
  
  //list<key, opType> access history, most recent at the back
  //opType = write => true
  //opType = read  => false
  typedef std::list<std::pair<key_type, bool> > key_list_type; 

  //key to (value and history iterator) map
  typedef MAP<
    key_type,
    std::pair<
      value_type*,
      typename key_list_type::iterator
      >
    > key_map_type;  

  ConcurrentQueue<key_type>* writeReqQueue;   //write request queue
  
  //------------------------------------------------------------------------------
  //Constuctor 
  CacheImpl(size_t sMax, mgm_cache* fc):
    mgmCache(fc),
    sizeMax(sMax),
    currentSize(0),
    writesSize(0),
    oldSizeQ(0),
    inLimitRegion(false)
  {
    recycleQueue = new ConcurrentQueue<value_type*>();
    writeReqQueue = new ConcurrentQueue<key_type>();
    
    pthread_mutex_init(&mutexList, NULL);
    pthread_rwlock_init(&rwMapLock, NULL);

    pthread_mutex_init(&mutexWriteDone, NULL);
    pthread_cond_init(&condWriteDone, NULL);

    pthread_mutex_init(&mutexWritesSize, NULL);
  }

  //------------------------------------------------------------------------------
  //Destructor
  ~CacheImpl()
  {
    //call destructor of elements in the map
    pthread_rwlock_wrlock(&rwMapLock);
    for (typename key_map_type::iterator it = keyValueMap.begin();
         it != keyValueMap.end();
         it++)
    {
      delete it->second.first;      
    }

    keyValueMap.clear();
    pthread_rwlock_unlock(&rwMapLock);

    //delete recyclabe objects
    value_type* ptr = NULL;
    while (recycleQueue->try_pop(ptr)) {
      delete ptr;
    }
    delete recycleQueue;
    delete writeReqQueue;
    
    pthread_mutex_destroy(&mutexList);
    pthread_rwlock_destroy(&rwMapLock);

    pthread_mutex_destroy(&mutexWriteDone);
    pthread_cond_destroy(&condWriteDone);

    pthread_mutex_destroy(&mutexWritesSize);
  }

  //------------------------------------------------------------------------------
  //Get value for key k from cache, return NULL if not found
  value_type*
  GetReadEntry(const key_type& k, file_abst* pFileAbst)
  {
    pthread_rwlock_rdlock(&rwMapLock);               //read lock map
    while (pFileAbst->GetNoWrites() != 0)
    {
      //wait until writes on current file are done
      pthread_rwlock_unlock(&rwMapLock);
      pFileAbst->WaitFinishWrites();
      pthread_rwlock_rdlock(&rwMapLock);
    }

    const typename key_map_type::iterator it = keyValueMap.find(k);
    if (it == keyValueMap.end())  
    {
      //key not found
      pthread_rwlock_unlock(&rwMapLock);             //unlock map
      return NULL;
    }
    else {
      // update access record
      pthread_mutex_lock(&mutexList);                //lock list
      keyList.splice(keyList.end(), keyList, it->second.second);
      pthread_mutex_unlock(&mutexList);              //unlock list
      pthread_rwlock_unlock(&rwMapLock);             //unlock map
      return it->second.first;
    }
  }


  //------------------------------------------------------------------------------
  //Proccess a write request from the qeue
  bool
  ProcessWriteReq(const key_type& k, int& retc)
  {
    double percentEpsilon = 0.1;
    double threshold = minPercentWrites * sizeMax;
    double epsilon =  threshold * percentEpsilon;
    size_t newSizeQ = writeReqQueue->GetSize();

    //delay the writing thread if there are not enough request
    //in the queue for the current file and then start processing them
    if (oldSizeQ != 0)
    {
      /*fprintf(stdout, "oldSize = %zu, newSize = %zu, writeSize = %zu, \
                limit = %f, currentSize = %zu \n", oldSizeQ, newSizeQ, GetWritesSize(),
               minPercentWrites * sizeMax, GetCurrentSize());
      */
      
      if ((oldSizeQ > newSizeQ) &&
          !inLimitRegion && 
          (GetWritesSize() < threshold + epsilon ) &&
          (GetWritesSize() > threshold - epsilon))
      {
        //downwards trend
        struct timespec ts;
        ts.tv_sec = 0;
        ts.tv_nsec = 500 * 1e6;  //500 mili sec
        //fprintf(stdout, "[%s] Writing thread sleep!\n", __FUNCTION__);
        nanosleep(&ts, NULL);
      }
    }
    oldSizeQ = newSizeQ;
    
    if ((GetWritesSize() < threshold + epsilon ) &&
        (GetWritesSize() > threshold - epsilon))
    {
      inLimitRegion = true;
    }
    else {
      inLimitRegion = false;
    }    
    
    value_type* pEntry = NULL;
    pthread_rwlock_rdlock(&rwMapLock);               //read lock map
    const typename key_map_type::iterator it = keyValueMap.find(k);
    if (it == keyValueMap.end())  
    {
      //key not found
      pthread_rwlock_unlock(&rwMapLock);             //unlock map
      return false;
    }
    else {
      typename key_list_type::iterator iterList;
      pEntry = it->second.first;
      iterList = it->second.second;
      pthread_rwlock_unlock(&rwMapLock);             //unlock map
     
      //do actual writing
      retc = XrdPosixXrootd::Pwrite(pEntry->GetFd(), pEntry->GetDataBuffer(), pEntry->GetLength(),
                            static_cast<long long>(pEntry->GetOffset()));

      //put error code in error queue
      if (retc != pEntry->GetOffset())
      {
        error_type elem = std::make_pair(
            retc, std::make_pair<off_t, size_t>(pEntry->GetOffset(), pEntry->GetLength()));
        pEntry->GetParentFile()->errorsQueue->push(elem);
      }
      
      //modify the state of the block in the list WRITE -> READ
      pthread_mutex_lock(&mutexList);                //lock list
      iterList->second = false;
      pthread_mutex_unlock(&mutexList);              //unlock list

      //decrease the number of write req pending for current file and in cache
      pEntry->GetParentFile()->DecrementWrites(pEntry->GetLength());
      DecreaseWritesSize(pEntry->GetLength());

      if (GetWritesSize() <= maxPercentWrites * sizeMax)
      {
        //notify possible waiting threads that a write was done
        //(i.e. possible free space in cache available)
        pthread_mutex_lock(&mutexWriteDone);
        pthread_cond_broadcast(&condWriteDone);        //send broadcast
        pthread_mutex_unlock(&mutexWriteDone);
      }
      
      return true;
    }
  }
  
  //------------------------------------------------------------------------------
  //Add a new write request to the queue
  void
  AddWrite(const key_type& k, value_type* const v)
  {
    pthread_rwlock_wrlock(&rwMapLock);            //write lock map
    pthread_mutex_lock(&mutexList);               //lock list

    size_t length = v->GetLength();
    while (GetCurrentSize() + length >= sizeMax) {
      if (!evict())
      {
        //release lock and wait for writing thread to do some writing
        //in case all cache is full with blocks for writing
        pthread_mutex_unlock(&mutexList);         //unlock list
        pthread_rwlock_unlock(&rwMapLock);        //unlock map

        pthread_mutex_lock(&mutexWriteDone);
        pthread_cond_wait(&condWriteDone, &mutexWriteDone);
        pthread_mutex_unlock(&mutexWriteDone);
        
        //relock the map and the list
        pthread_rwlock_wrlock(&rwMapLock);         //write lock map
        pthread_mutex_lock(&mutexList);            //lock list
      }
    }

    //check for overlap with other blocks
    typename key_map_type::iterator iter = keyValueMap.lower_bound(k);
    typename key_map_type::iterator aux_iter;
    off_t offStart;
    off_t offEnd;
    off_t offBlockEnd = v->GetOffset() + v->GetLength();
    
    if (iter != keyValueMap.end())
    {
      if (iter != keyValueMap.begin())
      {
        //if not first element in map then go one step back
        iter--;
      }
    
      for (int i = 0; ; i++)
      {
        offStart = iter->second.first->GetOffset();
        offEnd = offStart + iter->second.first->GetLength();

        //if current block overlaps with new block and is a reading block -> remove
        if ((((v->GetOffset() >= offStart) && (v->GetOffset() <= offEnd)) ||
             ((offBlockEnd >= offStart) && (offBlockEnd <= offEnd))) &&
            (iter->second.second->second == false))
        {
          //remove block from cache
          aux_iter = iter;
          aux_iter++;

          if (aux_iter != keyValueMap.end())
          {
            //if not last element then increment iterator
            currentSize -= iter->second.first->GetLength();
            keyList.erase(iter->second.second);
            keyValueMap.erase(iter);
            iter = aux_iter;
          }
          else 
            {
              //if last element then delete and exit loop
              currentSize -= iter->second.first->GetLength();
              keyList.erase(iter->second.second);
              keyValueMap.erase(iter);
              break;
            }
        }
        else
        { //no overlap with the current block
          iter++;
          if (iter == keyValueMap.end()) {
            break;
          }
        
          //if we are after the insertion point and the block does not
          //overlap with the new one there is no need to check further => exit loop
          if (i >= 2) break;
        }
      }
    }

    //increase the number of write requests on current file
    v->GetParentFile()->IncrementWrites(v->GetLength());
    v->GetParentFile()->IncrementNoBlocks();
      
    //update most-recently-used key
    typename key_list_type::iterator it =
        keyList.insert(keyList.end(), std::make_pair(k, true));
    currentSize += length;
    pthread_mutex_unlock(&mutexList);          //unlock list

    //add new entry
    keyValueMap.insert(std::make_pair(k, std::make_pair(v, it)));

    //update the size of write requests in cache
    IncreaseWritesSize(v->GetLength());
    pthread_rwlock_unlock(&rwMapLock);         //unlock map
    
    //add the new key to the queue for the write thread
    writeReqQueue->push(const_cast<key_type&>(k));
  }
 
  
  //------------------------------------------------------------------------------
  //Insert fresh key-value pair in the cache
  void
  Insert(const key_type& k, value_type* const v)
  {
    //decide if current block is to be added in the cache
    bool addToCache = false;    

    pthread_rwlock_wrlock(&rwMapLock);                  //write lock map
    pthread_mutex_lock(&mutexList);                     //lock list

    //if offset-end already in cache but smaller block then remove
    const typename key_map_type::iterator iterMap = keyValueMap.find(k);
    if (iterMap != keyValueMap.end()){
      value_type* ptr = iterMap->second.first;
      const typename key_list_type::iterator iterList = iterMap->second.second;

      //if block in cache of smaller and read type, then remove it
      if ((ptr->GetLength() < v->GetLength()) && (iterList->second == false) )
      {
        //decrease size of cache
        currentSize -= ptr->GetLength();
        keyValueMap.erase(iterMap);
        keyList.erase(iterList);
        
        //add block to the recycle pool
        recycleQueue->push(ptr);      
        addToCache = true;
      }
    }
    else {
      //block not in cache -> add it
      addToCache = true;
    }

    if (addToCache)
    { 
      size_t length = v->GetLength();
      while (GetCurrentSize() + length >= sizeMax) {
        if (!evict())
        {
          //release locks and wait for writing thread to do some writing
          //in case all cache is full with blocks for writing
          pthread_mutex_unlock(&mutexList);                //unlock list
          pthread_rwlock_unlock(&rwMapLock);               //unlock map

          pthread_mutex_lock(&mutexWriteDone);
          pthread_cond_wait(&condWriteDone, &mutexWriteDone);
          pthread_mutex_unlock(&mutexWriteDone);
         
          pthread_rwlock_wrlock(&rwMapLock);               //write lock map
          pthread_mutex_lock(&mutexList);                  //lock list       
        }
      }
     
      //update most-recently-used key
      typename key_list_type::iterator it =
        keyList.insert(keyList.end(), std::make_pair(k, false));

      //increment no of blocks for file in cache
      v->GetParentFile()->IncrementNoBlocks();
      
      pthread_mutex_unlock(&mutexList);                     //unlock list
      currentSize += length;

      //add new entry
      keyValueMap.insert(std::make_pair(k, std::make_pair(v,it)));
      pthread_rwlock_unlock(&rwMapLock);                    //unlock map
    }
    else 
    {
      pthread_mutex_unlock(&mutexList);                     //unlock list 
      pthread_rwlock_unlock(&rwMapLock);                    //unlock map
    }
  }

  
  //------------------------------------------------------------------------------
  value_type*
  GetRecycledObj(int filed, char* buf, off_t offset, size_t length, file_abst* pFileAbst)
  {
    value_type* pRecycledObj = NULL;
    
    if (recycleQueue->try_pop(pRecycledObj))
    {
      //got obj from pool
      pRecycledObj->Recycle(filed, buf, offset, length, pFileAbst);
    }
    else
    { //no obj in pool, allocate new one
      pRecycledObj = new value_type(filed, buf, offset, length, pFileAbst);
    }

    return pRecycledObj;
  }

  
  //------------------------------------------------------------------------------
  size_t GetCurrentSize() const { return currentSize; };
  void SetSize(size_t sMax){ sizeMax = sMax; };

  //------------------------------------------------------------------------------
  size_t GetWritesSize()
  {
    size_t lSize;
    pthread_mutex_lock(&mutexWritesSize);
    lSize = writesSize;
    pthread_mutex_unlock(&mutexWritesSize);
    return lSize;
  };


  //------------------------------------------------------------------------------
  void IncreaseWritesSize(size_t size)
  {
    pthread_mutex_lock(&mutexWritesSize);
    writesSize += size;
    pthread_mutex_unlock(&mutexWritesSize);
  };


  //------------------------------------------------------------------------------
  void DecreaseWritesSize(size_t size)
  {
    pthread_mutex_lock(&mutexWritesSize);
    writesSize -= size;
    pthread_mutex_unlock(&mutexWritesSize);
  };
  
  //------------------------------------------------------------------------------

 private:

  //------------------------------------------------------------------------------
  //Remove least-recently-used element in the cache
  bool
  evict()
  {
    bool foundCandidate = false;
    typename key_list_type::iterator iter = keyList.begin();
    for ( ; iter != keyList.end(); iter++)
    {
      if (iter->second == false)
      {
        //evict a read block, never evict a write block
        foundCandidate = true;
        break;
      }
    }

    if (foundCandidate)
    {
      const typename key_map_type::iterator it = keyValueMap.find(iter->first);
      if (it == keyValueMap.end())
        fprintf(stderr, "[evict] Iterator to the end!!\n");
      
      value_type* ptr = static_cast<value_type*>(it->second.first);
      currentSize -= ptr->GetLength();
      keyValueMap.erase(it);
      keyList.erase(iter);
          
      //remove file id from mapping if no more blocks in cache and 
      //there are no references to the file object
      if ((ptr->GetParentFile()->GetDecrementNoBlocks() == 0) && 
          (ptr->GetParentFile()->GetNoReferences() == 0))
      {
        mgmCache->RemoveFileInode(ptr->GetParentFile()->GetInode());
      }
           
      //add block to the recycle pool
      recycleQueue->push(ptr);      
    }
    
    return foundCandidate;
  }

  //------------------------------------------------------------------------------
  
  //Percentage of the total cache size which represents the upper limit
  //to which we accept new write requests, after this point notifications
  //to threads that want to submit new req are delayed
  static const double maxPercentWrites = 0.75;

  //Percent of the total cache size which represents the lower limit
  //from which the thread will wait for other write requests before 
  //executing the current one
  static const double minPercentWrites = 0.025;  

  mgm_cache*     mgmCache;         //upper mgm layer of the cache
  
  size_t         sizeMax;          //maximum size of cache
  size_t         currentSize;      //current size of cache
  size_t         writesSize;       //the size of write requests currently in cache
  key_list_type  keyList;          //list of recently accessed entries
  key_map_type   keyValueMap;      //map <key pair<value, listIter>>
  size_t         oldSizeQ;         //size queue write requests at previous step
  bool           inLimitRegion;    //mark if value in limit region

  pthread_mutex_t  mutexList;
  pthread_rwlock_t rwMapLock;

  pthread_mutex_t mutexWritesSize; //mutex for updating the size of write requests 
  pthread_mutex_t mutexWriteDone;  //mutex and condition for notifying possible
  pthread_cond_t condWriteDone;    //  waiting threads that a write op. has been done
  
  ConcurrentQueue<value_type*>* recycleQueue;   //pool of reusable objects 
};

#endif
