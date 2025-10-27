import functools

def check_connection(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if not self.connection or not self.cursor:
            raise ConnectionError(
                f"Ошибка (Метод: {func.__name__}): "
                f"Нет активного подключения к БД, вызовите connect()"
            )
        return func(self, *args, **kwargs)
    return wrapper