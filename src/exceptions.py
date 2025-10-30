DEFAULT_EXCEPTION_FORMAT = '[{prefix}-{code}]: {exception_msg}'

class BaseDBException(BaseException):
    prefix: str = 'DB'
    format: str = DEFAULT_EXCEPTION_FORMAT
    code = '000'
    msg = 'BASE DB EXCEPTION ERROR'
    exception: str = ''
    
    def __init__(self):
        self.format = DEFAULT_EXCEPTION_FORMAT
        self.exception = self.format.format(
            prefix=self.prefix,
            code=self.code,
            exception_msg=self.msg
        )
        
    def __str__(self):
        return self.exception