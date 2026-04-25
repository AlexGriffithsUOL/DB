import enum
import uuid
from .utils import BOOTSTRAP_TX_ID

class TransactionStatus(enum.Enum):
    ACTIVE = 'ACTIVE'
    COMMITTED = 'COMMITTED'
    ABORTED = 'ABORTED'

class Transaction:
    def __init__(self, tx_id, start_snapshot):
        self.id = tx_id
        self.start_snapshot = start_snapshot
        self.status = TransactionStatus.ACTIVE
        self.changes = []
        
    def __repr__(self):
        return f'{self.status.value} Transaction(id={self.id}, start_snapshot={self.start_snapshot})'
    
    def commit(self):
        self.status = TransactionStatus.COMMITTED

class TransactionManager:
    def __init__(self):
        self.transactions = dict()
        self.last_tx_id = BOOTSTRAP_TX_ID
        self.current_tx_id = BOOTSTRAP_TX_ID
        
    def filter_by_status(self, status):
        return [x.id for x in self.transactions.values() if x.status == status]
        
    @property
    def active_transactions(self):
        return self.filter_by_status(TransactionStatus.ACTIVE)
    
    @property
    def committed_transactions(self):
        return self.filter_by_status(TransactionStatus.COMMITTED)
    
    def get_last(self, transaction_list, func):
        if len(transaction_list) == 0:
            return self.last_tx_id
        else:
            return func(transaction_list)
    
    @property
    def last_committed_transaction_id(self):
        return self.get_last(self.committed_transactions, max)
        
    def add_to_transactions(self, transaction_id, transaction):
        self.transactions[transaction_id] = transaction
        
    def next_transaction_id(self):
        self.last_tx_id = self.current_tx_id
        self.current_tx_id += 1
        return self.current_tx_id
    
    @property
    def active_transaction(self):
        if len(self.active_transactions) == 0:
            return self.last_tx_id
        else:
            return min(self.active_transactions)

    def get_new_transaction(self):
        transaction_id = self.next_transaction_id()
        
        if transaction_id == 12:
            print('I forgot to fucking commit?')
        
        new_transaction = Transaction(transaction_id, self.last_committed_transaction_id)
        self.add_to_transactions(transaction_id, new_transaction)
        return new_transaction
    
    def get_transaction(self, tx_id):
        if tx_id in self.active_transactions:
            return self.transactions[tx_id]
        else:
            return None
        
GLOBAL_TRANSACTION_MANAGER = TransactionManager()

def get_transaction_manager():
    return GLOBAL_TRANSACTION_MANAGER
