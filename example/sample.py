from pykv.main import KeyValueStore

kv_store = None


def put_key(key, value):
    print(f"Putting {key}")
    kv_store.write(key, value)


def get(key):
    print(f"Getting {key}")
    return kv_store.read(key)


if __name__ == "__main__":
    print("Key value store initializing ..")
    kv_store = KeyValueStore()
    kv_store.start()
    print("Key value store initialized")

    tasks = []
    inputs = {
        "key_1": {'1': 2, '3': 55, '4': {'1': 2, '3': 55}},
        "key_2": {'1': 1000 * 'amuthan', '3': 55},
        "key_3": {'1': 'ss', '3': 55}
    }

    for k, v in inputs.items():
        put_key(k, v)

    assert get('key_1') == {'1': 2, '3': 55, '4': {'1': 2, '3': 55}}
    assert get('key_2') == {'1': 1000 * 'amuthan', '3': 55}
    assert get('key_3') == {'1': 'ss', '3': 55}

    assert kv_store.get_all() == {
        "key_1": {'1': 2, '3': 55, '4': {'1': 2, '3': 55}},
        "key_2": {'1': 1000 * 'amuthan', '3': 55},
        "key_3": {'1': 'ss', '3': 55}
    }

    print("Shutting down Key value store")
    kv_store.close()
