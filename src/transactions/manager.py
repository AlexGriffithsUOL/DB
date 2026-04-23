import enum
import uuid

class TransactionStatus(enum.Enum):
    ACTIVE = 'ACTIVE'
    COMMITTED = 'COMMITTED'
    ABORTED = 'ABORTED'

class TransactionManager:
    def __init__(self):
        self.transactions = dict()
        
    def add_to_transactions(self, transaction_id, status):
        self.transactions[transaction_id] = status

    def get_new_transaction(self):
        new_transaction_id = str(uuid.uuid4())
        status = TransactionStatus.ACTIVE.value
        self.add_to_transactions(new_transaction_id, status)
        
    pass