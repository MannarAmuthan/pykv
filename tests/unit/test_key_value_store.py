from unittest.mock import Mock

from pykv.main import KeyValueStore


def test_should_read_and_write_in_key_value_store():
    mocked_file_handler = Mock()

    kv_store = KeyValueStore(file_path="db.test",
                             file_handler=mocked_file_handler)

    kv_store.write("key_1", {'1': 2, '3': 55})

    assert kv_store.read('key_1') == {'1': 2, '3': 55}
    mocked_file_handler.create_file_if_not_exists.assert_called_once_with(file_path="db.test")
