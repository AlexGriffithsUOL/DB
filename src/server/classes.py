from src.table_manager.manager import TableManager, get_table_manager

class Server:
    def __init__(self, table_manager = None):
        if table_manager is None:
            self.table_manager = get_table_manager()
        else:
            self.table_manager = table_manager
        
    def process_request(self, dictionary):
        if 'action' in dictionary:
            if dictionary['action'] == 'select':
                transaction = self.table_manager.transaction_manager.get_new_transaction()
                table = dictionary['from']
                where = dictionary['where']
                column = dictionary['column']
                result = self.table_manager.tables[table].scan(
                    lambda x: x[column] == where,
                    transaction.start_snapshot,
                    index_column = column,
                    index_value = where
                )
                return result