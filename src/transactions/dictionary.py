TRANSACTION_WORDS = {
    'BEGIN'
}

class TransactionWord:
    value: str
    pass

class TXBegin(TransactionWord):
    value = 'BEGIN'
    
class TXCommit(TransactionWord):
    value = 'COMMIT'
    
class TXEnd(TransactionWord):
    value = 'END'