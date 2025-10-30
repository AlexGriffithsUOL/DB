from ..exceptions import BaseDBException

class BaseFileManagerException(BaseDBException):
    prefix: str = 'FM'
    code = '000'
    msg = 'BASE FILE MANAGER EXCEPTION'

class FileManagerNoConfigException(BaseFileManagerException):
    code = '001'
    msg = 'File Manager instanciated with no configs'
    
class UnderMinimumPageSizeException(BaseFileManagerException):
    code = '002'
    msg = 'File Manager page size below minimum ({min_size} bytes) at {current_size} bytes'
    
    def __init__(self, min_size, current_size):
        self.msg = self.msg.format(min_size=min_size, current_size=current_size)
        super().__init__()
        
class DBFileOpenFailure(BaseFileManagerException):
    code = '003'
    msg = 'File Manager has failed to open database file, expected at {db_location}'
    
    def __init__(self, db_location):
        self.msg = self.msg.format(db_location=db_location)
        super().__init__()
        
class FileManagerReadPageException(BaseFileManagerException):
    code = '004'
    msg = 'File Manager has failed to read page {page_id}'
    
    def __init__(self, page_id):
        self.msg = self.msg.format(page_id=page_id)
        super().__init__()
        
class CannotOpenFileException(BaseFileManagerException):
    code = '005'
    msg = 'File Manager cannot open database file'