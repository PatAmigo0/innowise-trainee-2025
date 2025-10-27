import functools
import os

def _isdir(dir_path: str):
    return os.path.isdir(dir_path)

def isdir(path):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not _isdir(path):
                print(f"[ Error ]: директория не найдена ({path})")
                raise FileNotFoundError
            return func(*args, **kwargs)
        return wrapper
    return decorator
