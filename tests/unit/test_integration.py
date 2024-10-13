import threading

from pykv.main import KeyValueStore


def test_kv_store():
    kv_store = KeyValueStore()
    kv_store.start()

    tasks = []
    inputs = {
        "key_1": {'1': 2, '3': 55, '4': {'1': 2, '3': 55}},
        "key_2": {'1': 1000 * 'amuthan', '3': 55},
        "key_3": {'1': 'ss', '3': 55}
    }

    for k, v in inputs.items():
        task = threading.Thread(target=kv_store.write, args=(k, v))
        tasks.append(task)
        task.start()

    for t in tasks:
        t.join()

    assert kv_store.read('key_1') == {'1': 2, '3': 55, '4': {'1': 2, '3': 55}}
    assert kv_store.read('key_2') == {'1': 1000 * 'amuthan', '3': 55}
    assert kv_store.read('key_3') == {'1': 'ss', '3': 55}

    assert kv_store.get_all() == {
        "key_1": {'1': 2, '3': 55, '4': {'1': 2, '3': 55}},
        "key_2": {'1': 1000 * 'amuthan', '3': 55},
        "key_3": {'1': 'ss', '3': 55}
    }

    kv_store.close()
