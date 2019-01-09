from ocdskingfisherprocess.util import parse_string_to_date_time


def test_parse_string_to_date_time_1():
    date = parse_string_to_date_time("2019-04-01 10:11:12")
    assert "2019-04-01 10-11-12" == date.strftime("%Y-%m-%d %H-%M-%S")


def test_parse_string_to_date_time_2():
    date = parse_string_to_date_time("2019-04-01-10-11-12")
    assert "2019-04-01 10-11-12" == date.strftime("%Y-%m-%d %H-%M-%S")
