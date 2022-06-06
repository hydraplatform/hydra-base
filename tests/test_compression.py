import codecs
import base64
import pytest

from hydra_base.util.compression import (
    block_sz,
    bz2b64compress,
    b64bz2decompress
)


src = """
Some arbitrary text to test bz2 compression funcs with, long
enough that the compression should result in a signficant
decrease in length of the compressed text relative to the
original text.
"""

@pytest.fixture
def input():
    """ This gives a 1546123 byte input """
    return src * 8011


def test_has_codecs():
    """
    Are the codecs required for compression available, and does
    each specify both an incremental encoder/decoder for block-
    based operations?
    """
    required_codecs = ("utf8", "bz2", "base64")

    for encoding in required_codecs:
        # Raises LookupError on missing codec
        codec = codecs.lookup(encoding)
        assert codec.incrementalencoder is not None
        assert codec.incrementaldecoder is not None


def test_compression(input):
    """
    Does compression of the canonical input result in a valid
    base64 string with the expected characteristics?
    """
    compressed = bz2b64compress(input)
    # Output is string
    assert isinstance(compressed, str)
    # Output has expected length
    assert len(compressed) == 1360
    # Output is valid base64
    bz2_bytes = base64.b64decode(compressed)
    isinstance(bz2_bytes, bytes)


def test_decompression(input):
    """
    Does compression followed by decompression result in the
    original input?
    """
    compressed = bz2b64compress(input)
    original = b64bz2decompress(compressed)

    assert input == original


def test_irregular_block_sz(input):
    """
    The base64 produced by bz2b64compress is padded to a four-byte
    terminal block.  This test confirms that the decompressor handles
    overflow correctly where the block size for each pass is not
    divisible by four.
    """
    global block_sz
    block_sz = 333

    compressed = bz2b64compress(input)
    original = b64bz2decompress(compressed)

    assert input == original
