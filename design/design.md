
## Key Value Store Design 

Pykv is a file based key-value store, which uses **memory-mapped io** for fast read and write the key values in a store.
If file is already presents in a path, it loads the file from same, otherwise it creates the new store file.
(look into the unit test `test_should_load_existing_file` ) . All the file read and write operations are **thread safe**.

## How data is stored 

Store file is made up of _records_  

#### Record structure

A record can be single slot or multiple slots , based on value size.

```
TYPE_FLAG = int  # 1 byte , 0 - Empty , 1 - Primary, 2 - Sub slot
SLOTS_COUNT = int  # 1 byte , length of slots for this record, applicable only for primary slot
TIME_TO_LIVE = int  # 4 byte, UNIX Epoch timestamp
KEY_LEN = int  # 1 byte , length of key byte string
VAL_LEN = int  # 2 byte , length of value byte string
KEY_BYTES = bytes
VALUE_BYTES = bytes

Record = Tuple[TYPE_FLAG, SLOTS_COUNT, TIME_TO_LIVE, KEY_LEN, VAL_LEN, KEY_BYTES, VALUE_BYTES]
```

If one record is not enough for storing a value, then multiple contiguous records are utilized. In the case TYPE_FLAG is 2, which is sub slot.
This fixed size of records will benefit us in terms of managing and iterating keys.



#### More internals

1. Keys and their record-offsets will be managed in program memory always.
      example: key_one : 234
2. If key is deleted , then it will be removed from this dictionary and the respective record(s) will be flagged as _deleted/empty_.
3. Whenever key is read, store will check if it is expired, if yes then an exception will be raised. 
4. Background job will always run to mark key as _empty_ if they are expired by TTL time. 
5. [TODO] : Currently there is an increment pointer to store the offset of last stored record, which will then incremented in which the new key will be stored. 
This should also consider to use emptied records which is available to store new key-values. This can be achieved by interval data structures.
6. [TODO]: Capability to save/export the store-file upon closing the application. 
7. [TODO]: Limit number of slots to restrict the file by 1GB.




