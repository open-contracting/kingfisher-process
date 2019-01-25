import time
import requests
import json
import hashlib
import datetime
import tempfile
import os

RETRY_TIME = 10


def get_hash_md5_for_data(data):
    data_str = json.dumps(data, sort_keys=True)
    return hashlib.md5(data_str.encode('utf-8')).hexdigest()


def get_url_request(url, headers=None, stream=False, tries=1, errors=None, verify_ssl=True):
    '''
    Handle transient network errors, and URLs with
    intermittent timeouts.
    '''
    if not errors:
        errors = []

    error_msg = None

    if tries > 3:
        return (None, errors)
    try:
        r = requests.get(url, headers=headers, stream=stream, verify=verify_ssl)
        r.raise_for_status()
    except requests.exceptions.Timeout:
        error_msg = 'Request timeout'
    except requests.ConnectionError:
        error_msg = 'Connection error'
    except requests.exceptions.TooManyRedirects:
        error_msg = 'Too many redirects'
    except requests.exceptions.RequestException as e:
        error_msg = 'Request exception (Code %s): %s' % (r.status_code, e)

    if not error_msg:
        return r, []

    # only add to errors list if it is different from last error.
    if not errors or (errors and errors[-1] != error_msg):
        errors.append(error_msg)

    time.sleep(RETRY_TIME)
    return get_url_request(url, headers, stream, tries + 1, errors)


control_codes_to_filter_out = [
    b'\\u0000',
    b'\x02',
    b'\x03',
    b'\x04',
    b'\x05',
    b'\x06',
    b'\x07',
    b'\x08',
    b'\x09',
    b'\x0B',
    b'\x0C',
    b'\x0D',
    b'\x0E',
    b'\x0F',
    b'\x10',
    b'\x11',
    b'\x12',
    b'\x13',
    b'\x14',
    b'\x15',
    b'\x16',
    b'\x17',
    b'\x18',
    b'\x19',
    b'\x1A',
    b'\x1B',
    b'\x1C',
    b'\x1D',
    b'\x1E',
    b'\x1F',
]


def control_code_to_filter_out_to_human_readable(control_code_to_filter_out):
    if len(control_code_to_filter_out) == 1:
        return 'chr('+str(ord(control_code_to_filter_out))+')'
    else:
        return control_code_to_filter_out


class FileToStore:

    def __init__(self, source_filename, encoding='utf-8'):
        self.source_filename = source_filename
        self.processed_filename = None
        self.warnings = []
        # We don't actually do anything with encoding yet, but we might need to later ...
        self.encoding = encoding

    def __enter__(self):
        if self._have_to_process_file():
            (fp_write, fn_write) = tempfile.mkstemp(prefix='tmp_kingfisher_process_')

            with open(self.source_filename, 'rb') as fp_read:
                while True:
                    chunk = fp_read.read(1024 ^ 2)
                    if not chunk:
                        break
                    for control_code_to_filter_out in control_codes_to_filter_out:
                        if control_code_to_filter_out in chunk:
                            chunk = chunk.replace(control_code_to_filter_out, b'')
                            warning = 'We had to replace control codes: ' \
                                      + control_code_to_filter_out_to_human_readable(control_code_to_filter_out)
                            if warning not in self.warnings:
                                self.warnings.append(warning)
                    os.write(fp_write, chunk)

            os.close(fp_write)
            self.processed_filename = fn_write

        return self

    def _have_to_process_file(self):
        with open(self.source_filename, 'rb') as fp_read:
            while True:
                chunk = fp_read.read(1024 ^ 2)
                if not chunk:
                    break
                for control_code_to_filter_out in control_codes_to_filter_out:
                    if control_code_to_filter_out in chunk:
                        return True
        return False

    def get_filename(self):
        if self.processed_filename:
            return self.processed_filename
        else:
            return self.source_filename

    def get_warnings(self):
        return self.warnings

    def __exit__(self, type, value, traceback):
        pass


def save_content(url, filepath, headers=None, verify_ssl=True, replace_control_codes=True):
    request, errors = get_url_request(url, stream=True, headers=headers, verify_ssl=verify_ssl)

    if not request:
        return SaveContentResult(errors=errors)

    warnings = []
    try:
        with open(filepath, 'wb') as f:
            for chunk in request.iter_content(1024 ^ 2):
                for control_code_to_filter_out in control_codes_to_filter_out:
                    if replace_control_codes and control_code_to_filter_out in chunk:
                        chunk = chunk.replace(control_code_to_filter_out, b'')
                        warning = 'We had to replace control codes: ' \
                                  + control_code_to_filter_out_to_human_readable(control_code_to_filter_out)
                        if warning not in warnings:
                            warnings.append(warning)
                f.write(chunk)
        return SaveContentResult(warnings=warnings)
    except Exception as e:
        return SaveContentResult(errors=[str(e)], warnings=warnings)


class SaveContentResult:
    def __init__(self, warnings=[], errors=[]):
        self.errors = errors
        self.warnings = warnings


# if is_json:
#    try:
#        data = r.json()
#        return data, []
#    except json.JSONDecodeError:
#        error_msg = 'Failed to decode json'
# else:
#    try:
#        content = r.content
#        return content, []
#    except Exception as e:
#        error_msg = 'Unable to decode content: %s' % e


def parse_string_to_date_time(date_time_string):
    if " " in date_time_string:
        # The docs define this format
        return datetime.datetime.strptime(date_time_string, "%Y-%m-%d %H:%M:%S")
    else:
        # But some of our test code uses this format so we'll allow that too.
        return datetime.datetime.strptime(date_time_string, "%Y-%m-%d-%H-%M-%S")


def parse_string_to_boolean(boolean_string):
    return True if boolean_string and boolean_string in ['1', 'true', 't'] else False
