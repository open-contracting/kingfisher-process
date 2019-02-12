from ocdskingfisherprocess.util import parse_string_to_date_time, parse_string_to_boolean, FileToStore, \
    control_codes_to_filter_out, control_code_to_filter_out_to_human_readable
import os


def test_parse_string_to_boolean_1():
    assert True == parse_string_to_boolean("true") # noqa


def test_parse_string_to_boolean_2():
    assert False == parse_string_to_boolean("false") # noqa


def test_parse_string_to_boolean_3():
    assert True == parse_string_to_boolean("t") # noqa


def test_parse_string_to_boolean_4():
    assert False == parse_string_to_boolean("") # noqa


def test_parse_string_to_boolean_5():
    assert False == parse_string_to_boolean(None) # noqa


def test_parse_string_to_boolean_6():
    assert True == parse_string_to_boolean("True") # noqa


def test_parse_string_to_date_time_1():
    date = parse_string_to_date_time("2019-04-01 10:11:12")
    assert "2019-04-01 10-11-12" == date.strftime("%Y-%m-%d %H-%M-%S")


def test_parse_string_to_date_time_2():
    date = parse_string_to_date_time("2019-04-01-10-11-12")
    assert "2019-04-01 10-11-12" == date.strftime("%Y-%m-%d %H-%M-%S")


def test_file_to_store_sample_1_0_record_with_control_codes():
    json_filename = os.path.join(os.path.dirname(
        os.path.realpath(__file__)), 'data', 'sample_1_0_record_with_control_codes.json'
    )

    with FileToStore(json_filename) as file_to_store:
        # Processing is required in this file, so path should be different
        assert file_to_store.get_filename() != json_filename

        assert len(file_to_store.get_warnings()) == 1
        assert file_to_store.get_warnings()[0] == 'We had to replace control codes: chr(16)'


def test_file_to_store_sample_1_0_record():
    json_filename = os.path.join(os.path.dirname(
        os.path.realpath(__file__)), 'data', 'sample_1_0_record.json'
    )

    with FileToStore(json_filename) as file_to_store:
        # Processing is NOT required in this file, so path should be same
        assert file_to_store.get_filename() == json_filename

        assert len(file_to_store.get_warnings()) == 0


def test_control_code_to_filter_out_to_human_readable():
    for control_code_to_filter_out in control_codes_to_filter_out:
        # This test just calls it and make sure it runs without crashing
        # (some code was crashing, so wanted test to check all future values of control_codes_to_filter_out)
        # We add it to a string, as this is what happens in real code. This catches any "must be str, not bytes" errors.
        print(" " + control_code_to_filter_out_to_human_readable(control_code_to_filter_out))
