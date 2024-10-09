import os
from time import sleep

from _pytest.python_api import raises

from pykv.main import KeyValueStore, ExpiredKeyException


class TestKeyValueStore:
    file_path = None

    @classmethod
    def setup_class(cls):
        cls.file_path = "test_db.bin"

    @classmethod
    def teardown_method(cls):
        if os.path.exists(cls.file_path):
            os.remove(cls.file_path)

    def test_should_read_and_write_in_key_value_store(self):
        kv_store = KeyValueStore(file_path=self.file_path)

        kv_store.write("key_1", {'1': 2, '3': 55, '4': {'1': 2, '3': 55}})
        kv_store.write("key_2", {'1': 1000 * 'amuthan', '3': 55})
        kv_store.write("key_3", {'1': 'ss', '3': 55})

        assert kv_store.read('key_1') == {'1': 2, '3': 55, '4': {'1': 2, '3': 55}}
        assert kv_store.read('key_2') == {'1': 1000 * 'amuthan', '3': 55}
        assert kv_store.read('key_3') == {'1': 'ss', '3': 55}

        assert kv_store.get_all() == {
            "key_1": {'1': 2, '3': 55, '4': {'1': 2, '3': 55}},
            "key_2": {'1': 1000 * 'amuthan', '3': 55},
            "key_3": {'1': 'ss', '3': 55}
        }

    def test_should_delete_in_key_value_store(self):
        kv_store = KeyValueStore(file_path=self.file_path)

        kv_store.write("key_1", {'1': 2, '3': 55, '4': {'1': 2, '3': 55}})
        kv_store.write("key_2", {'1': 1000 * 'amuthan', '3': 55})
        kv_store.write("key_3", {'1': 'ss', '3': 55})

        kv_store.delete('key_2')

        assert kv_store.get_all() == {
            "key_1": {'1': 2, '3': 55, '4': {'1': 2, '3': 55}},
            "key_3": {'1': 'ss', '3': 55}
        }

    def test_should_load_existing_file(self):
        kv_store = KeyValueStore(file_path='tests/unit/test_data_fixture.bin')

        assert kv_store.get_all() == {
            "key_1": {'1': 2, '3': 55, '4': {'1': 2, '3': 55}},
            "key_3": {'1': 'ss', '3': 55}
        }

    def test_should_raise_exception_if_key_accessed_after_time_to_live_expired(self):
        kv_store = KeyValueStore(file_path=self.file_path)

        kv_store.write("key_1", {'1': 2, '3': 55, '4': {'1': 2, '3': 55}})
        kv_store.write("key_2", {'1': 1000 * 'amuthan', '3': 55}, time_to_live_in_seconds=3)
        kv_store.write("key_3", {'1': 'ss', '3': 55})

        sleep(4)

        with raises(expected_exception=ExpiredKeyException):
            kv_store.read("key_2")

        assert kv_store.get_all() == {
            "key_1": {'1': 2, '3': 55, '4': {'1': 2, '3': 55}},
            "key_3": {'1': 'ss', '3': 55}
        }
