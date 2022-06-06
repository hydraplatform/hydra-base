"""
Utilities to compress and decompress datasets.
"""

import codecs

# Retrieve only incremental codecs, raises LookupError on absence
utf8enc = codecs.getincrementalencoder("utf8")()
bz2enc = codecs.getincrementalencoder("bz2")()
b64enc = codecs.getincrementalencoder("base64")()

utf8dec = codecs.getincrementaldecoder("utf8")()
bz2dec = codecs.getincrementaldecoder("bz2")()
b64dec = codecs.getincrementaldecoder("base64")()

"""
Codecs in each of these categories must have their state reset between
the relevant operations. This allows reuse of a single incremental codec
instance of each type throughout the module.
"""
compression_codecs = (utf8enc, bz2enc, b64enc, utf8dec)
decompression_codecs = (utf8dec, bz2dec, b64dec, utf8enc)

block_sz = 512


def bz2b64compress(data: str) -> str:
    """
    Returns a base64 string representing the bz2 compressed bytes of the
    utf8 encoded input string.
    Base64 output is always padded to a multiple of four bytes.
    Newlines resulting from MIME compatible output are removed.
    """
    for codec in compression_codecs:
        codec.reset()
    i = 0
    output = []
    b64_overflow = b''
    is_final = False
    while not is_final:
        start = i*block_sz
        end = start + block_sz
        block = data[start:end]
        is_final = not len(block) == block_sz
        bdata = utf8enc.encode(block, final=is_final)
        bz2_enc = bz2enc.encode(bdata, final=is_final)
        pad_enc = b64_overflow + bz2_enc
        pad_sz = len(pad_enc)
        over = pad_sz % 3
        b64_block = pad_enc[:pad_sz-over]
        b64_overflow = pad_enc[pad_sz-over:]
        utf_block = b64enc.encode(b64_block, final=is_final)
        b64_enc = utf8dec.decode(utf_block, final=is_final)
        if b64_enc:
            output.append(b64_enc.replace('\n', ''))
        i += 1

    if b64_overflow:
        b64_enc = b64enc.encode(b64_overflow, final=True)
        output.append(b64_enc.replace(b'\n', b'').decode())

    return "".join(output)


def b64bz2decompress(data: str) -> str:
    """
    Returns the string resulting from a bz2 decompression of utf8
    encoded base64 input.
    """
    for codec in decompression_codecs:
        codec.reset()
    i = 0
    output = []
    is_final = False
    b64_overflow = b''
    while not is_final:
        start = i*block_sz
        end = start + block_sz
        block = data[start:end]
        is_final = not len(block) == block_sz
        b64_block = utf8enc.encode(block)
        pad_block = b64_overflow + b64_block
        pad_sz = len(pad_block)
        over = pad_sz % 4
        b64_in = pad_block[:pad_sz-over]
        b64_overflow = pad_block[pad_sz-over:]
        bz2_block = b64dec.decode(b64_in, is_final)
        utf_block = bz2dec.decode(bz2_block, is_final)
        dec = utf8dec.decode(utf_block)
        if dec:
            output.append(dec)
        i += 1

    return "".join(output)
