from src.exceptions import BaseDBException

class BaseCatalogHeaderException(BaseDBException):
    prefix = 'CH'
    code = '000'
    msg = 'BASE CATALOG HEADER EXCEPTION'
    
class CatalogHeaderInvalidDBFileException(BaseCatalogHeaderException):
    code = '001'
    msg = 'Invalid database file.'