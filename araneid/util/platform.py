from sys import platform

def is_windows():
    return platform.startswith('win32') or platform.startswith('cygwin')

def is_linux():
    return platform.startswith('linux')

def is_macos():
    return platform.startswith('darwin')