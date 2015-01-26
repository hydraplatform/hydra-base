import platform, sys

if platform.system() == 'Windows': # pragma: no cover
    WIN = True
else: # pragma: no cover
    WIN = False

# True if we are running on Python 3.
PY2 = sys.version_info[0] == 2

if not PY2: # pragma: no cover
    string_type = str
    unicode_text = str
    byte_string = bytes
else:
    string_type = basestring
    unicode_text = unicode
    byte_string = str
