from src.exceptions import BaseDBException

class BaseTransactionManagerException(BaseDBException):
    prefix = 'TM'
    code = '000'
    msg = 'BASE TRANSACTION MANAGER EXCEPTION'
    
class TransactionAlreadyCommittedException(BaseTransactionManagerException):
    code = '001'
    msg = 'Transaction already committed.'
    
class TransactionAbortedException(BaseTransactionManagerException):
    code = '002'
    msg = 'Transaction has been aborted.'