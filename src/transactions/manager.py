import enum
import uuid
from .utils import BOOTSTRAP_TX_ID
from .exceptions import (
    TransactionAlreadyCommittedException,
    TransactionAbortedException
)
from .actions import TXAction, TXActionType
import logging

tm_logger = logging.getLogger('TransactionManager')
tx_logger = logging.getLogger('Transaction')
logging.basicConfig(level=logging.DEBUG)

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
        self.timeline = []
        
    def __repr__(self):
        return f'{self.status.value} Transaction(id={self.id}, start_snapshot={self.start_snapshot})'
    
    def _format_log_(self, log):
        return f'[TX-{self.id}] {log}'
    
    def add_to_timeline(self, action):
        self.timeline.append(action)
    
    def commit(self):
        if self.status == TransactionStatus.COMMITTED:
            tx_logger.error(self._format_log_('Transaction already committed'))
            raise TransactionAlreadyCommittedException
        
        if self.status != TransactionStatus.ACTIVE:
            tx_logger.error(self._format_log_('Transaction aborted'))
            raise TransactionAbortedException
        
        tx_logger.info(self._format_log_('Timeline clearing'))
        
        for action in self.timeline:
            match (action.action_type):
                case (TXActionType.INDEX_INSERT):
                    action.index.insert(action.key, action.rid)
                    
                case (TXActionType.INDEX_DELETE):
                    action.index.delete(action.key, action.rid)

        self.timeline.clear()
        tx_logger.info(self._format_log_('Timeline cleared'))
        
        
        self.status = TransactionStatus.COMMITTED
        
    def rollback(self):
        tx_logger.info(self._format_log_('Beginning rollback'))
        
        if self.status != TransactionStatus.ACTIVE:
            tx_logger.error(self._format_log_('Transaction is not active'))
            self.status = TransactionStatus.ABORTED
        
        for i, action in enumerate(self.timeline):
            tx_logger.info(f'Rolling back {action.action_type.value} on {action.rid}')
            
            match action.action_type:
                case (TXActionType.INSERT):
                    tx_logger.info(self._format_log_(f'Matched case to {TXActionType.INSERT.value} branch'))
                    action.table.delete_at(*action.rid)
                    
                case (TXActionType.DELETE):
                    tx_logger.info(self._format_log_(f'Matched case to {TXActionType.DELETE.value} branch'))
                    action.table.insert_at(action.old_data, *action.rid)
                    
                case (TXActionType.UPDATE):
                    tx_logger.info(self._format_log_(f'Matched case to {TXActionType.UPDATE.value} branch'))
                    
                case (TXActionType.INDEX_DELETE):
                    tx_logger.info(self._format_log_(f'Matched case to {TXActionType.INDEX_DELETE.value} branch'))
                    # action.index.insert(action.key, action.rid)
                    
                case (TXActionType.INDEX_INSERT):
                    tx_logger.info(self._format_log_(f'Matched case to {TXActionType.INDEX_DELETE.value} branch'))
                    #index operations aren't needed as they don't exist yet
                    # action.index.delete(action.key, action.rid)
            
            
        self.status = TransactionStatus.ABORTED
        

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
