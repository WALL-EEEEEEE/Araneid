
def cast_exception_to(source: Exception, target: Exception)-> Exception:
    assert isinstance(source, Exception) and isinstance(target, Exception)
    final_exception: Exception = None
    if type(source) == type(target):
       final_exception = source
    else:
        try:
            raise target from source
        except Exception as e:
            final_exception = e
    return final_exception