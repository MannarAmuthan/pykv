from pykv.data_structures.interval import Interval


def test_should_delete_interval():
    interval = Interval([1, 1000])
    interval.delete([50, 500])
    assert interval.intervals == [[1, 49], [501, 1000]]

    interval = Interval([4000, 4002])
    interval.delete([4000, 4001])
    assert interval.intervals == [[4002, 4002]]


def test_should_query_interval():
    interval = Interval([1, 2])
    interval.insert([3, 5])
    interval.insert([7, 14])
    interval.insert([17, 27])

    assert interval.query(10) == [17, 27]

    interval = Interval([1, 2])
    interval.insert([3, 5])
    interval.insert([7, 15])
    interval.insert([17, 27])

    assert interval.query(8) == [7, 15]


def test_should_add_interval():
    interval = Interval([100, 1000])
    interval.insert([5, 50])
    interval.insert([70, 90])
    interval.delete([80, 85])
    interval.insert([80, 85])
    assert interval.intervals == [[5, 50], [70, 90], [100, 1000]]

    interval = Interval([4000, 4002])
    interval.insert([4002, 4010])
    assert interval.intervals == [[4000, 4010]]
