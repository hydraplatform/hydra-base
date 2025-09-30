import ctypes
import sys
import uuid


try:
    from ctypes import (
        windll,
        wintypes
    )
except ImportError as exc:
    raise Warning(f"Attempt to use Windows utils on {sys.platform} platform") from exc


# Pre-Vista CSIDL
CSIDL_COMMON_DOCUMENTS = 46
# Vista onwards Known Folder ID
# From https://learn.microsoft.com/en-us/windows/win32/shell/knownfolderid
KFP_PUBLIC_DOCUMENTS = uuid.UUID("{ED4824AF-DCE4-45A8-81E2-FC7965083634}")


class GUID(ctypes.Structure):
    # windsdk/guiddef.h
    _fields_ = [
        ("Data1", wintypes.DWORD),
        ("Data2", wintypes.WORD),
        ("Data3", wintypes.WORD),
        ("Data4", wintypes.BYTE*8)
    ]

    def __init__(self, _uuid):
        ctypes.Structure.__init__(self)
        self.Data1, self.Data2, self.Data3 = _uuid.fields[:3]
        self.Data4[:] = _uuid.bytes[8:]


def check_ret_err(result, func, args):
    if result != 0:
        raise OSerror("Unable to locate Common Documents")

    return args


def win_get_common_documents():
    # If the more recent KPF system is available, use this...
    if Win_SHGetKnownFolderPath := getattr(windll.shell32, "SHGetKnownFolderPath", None):
        Win_SHGetKnownFolderPath.argtypes = [
            ctypes.POINTER(GUID),
            wintypes.DWORD,
            wintypes.HANDLE,
            ctypes.POINTER(ctypes.c_wchar_p)
        ]
        Win_SHGetKnownFolderPath.errcheck = check_ret_err

        kfp = GUID(KFP_PUBLIC_DOCUMENTS)
        path_buf = ctypes.c_wchar_p()
        Win_SHGetKnownFolderPath(ctypes.byref(kfp), 0, 0, ctypes.byref(path_buf))

        return path_buf.value

    # ...otherwise use legacy CSIDL
    elif Win_SHGetFolderPathW := getattr(windll.shell32, "SHGetFolderPathW", None):
        Win_SHGetFolderPathW.argtypes = [
            wintypes.HWND,
            ctypes.c_int,
            wintypes.HANDLE,
            wintypes.DWORD,
            wintypes.LPCWSTR
        ]
        Win_SHGetFolderPathW.errcheck = check_ret_err

        path_buf = ctypes.create_unicode_buffer(wintypes.MAX_PATH)
        Win_SHGetFolderPathW(0, CSIDL_COMMON_DOCUMENTS, 0, 0, path_buf)

        return path_buf.value
    else:
        raise OSError("Unable to access Windows environment")


if __name__ == "__main__":
    cdp = win_get_common_documents()
    print(cdp)
