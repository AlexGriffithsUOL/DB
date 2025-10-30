from ..exceptions import BaseDBException

class BasePageManagerException(BaseDBException):
    prefix = 'PM'
    code = '000'
    msg = 'BASE PAGE MANAGER EXCEPTION'
    
class NoPageSizeExcConfigException(BasePageManagerException):
    code = '001'
    msg = 'No page size passed to Page on initialisation'
    
    