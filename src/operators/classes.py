import enum
from src.transactions.manager import Transaction
from src.table_manager.utils import single_filter_by_tx_snapshot
from src.records.structured_records import StructuredDataRecordPage
from src.parser.tokens import TOKENS

class EqualityOperatorEnum(enum.Enum):
    EQ = '=='
    GT = '>'
    GTE = '>='
    LT = '<'
    LTE = '<='
    BETWEEN = 'BETWEEN'
    
    @classmethod
    def LOWER(cls):
        return (cls.LT.value, cls.LTE.value)
    
    @classmethod
    def GREATER(cls):
        return (cls.GT.value, cls.GTE.value)
    
    @classmethod
    def INCLUSIVE(cls):
        return (cls.LTE.value, cls.GTE.value, cls.BETWEEN.value)
    
    @classmethod
    def EXCLUSIVE(cls):
        return (cls.LT.value, cls.GT.value)

class BaseOperation:
    def open(self):
        raise NotImplementedError()
    
    def next(self):
        raise NotImplementedError()
    
    def close(self):
        raise NotImplementedError()
    
class TableCursor:
    def __init__(self, table: "Table", tx: Transaction):
        self.table = table
        self.tx = tx
        self.current_page_id = table.first_page_id
        self.structured_page = StructuredDataRecordPage(
            table.page_allocator.get_page(self.current_page_id).data
        )
        self.slot_index = 0
        
    def next(self):
        while True:
            if self.slot_index < self.structured_page.num_slots:
                if not self.structured_page._slot_deleted(self.slot_index):
                    raw = self.structured_page.read_slot(self.slot_index)
                    row = self.table.deserialise(raw)
                    self.slot_index += 1
                    record = single_filter_by_tx_snapshot(row, self.tx.start_snapshot)
                    
                    if record is not None:
                        return record
                    
                else:
                    self.slot_index += 1
            else:
                if self.structured_page.next_page_id == -1:
                    return None
                self.current_page_id = self.structured_page.next_page_id
                page_data = self.table.page_allocator.get_page(self.current_page_id).data
                self.structured_page = StructuredDataRecordPage(page_data)
                self.structured_page.page_id = self.current_page_id
                self.slot_index = 0

class TableScan(BaseOperation):
    def __init__(self, table: "Table", tx: "Transaction"):
        self.table = table
        self.tx = tx
        self.cursor = TableCursor(table, tx)

    def open(self):
        self.cursor = TableCursor(self.table, self.tx)

    def next(self):
        return self.cursor.next()

    def close(self):
        pass
    
class IndexScan(BaseOperation):
    def __init__(self, table, index, operator: EqualityOperatorEnum, tx: Transaction, upper_bound_or_key = None, lower_bound = None):
        self.table = table
        self.index = index
        self.upper_bound_or_key = upper_bound_or_key
        self.lower_bound = lower_bound
        self.tx = tx
        self.locations = None
        self.pos = 0
        self.operator: EqualityOperatorEnum = operator

    def open(self):
        if self.operator == EqualityOperatorEnum.EQ.value:
            self.locations = self.index.search(self.upper_bound_or_key)
            
        if self.operator in EqualityOperatorEnum.LOWER():
            self.locations = self.index.range_scan(None, self.upper_bound_or_key, self.operator)
        
        if self.operator in EqualityOperatorEnum.GREATER():
            self.locations = self.index.range_scan(self.upper_bound_or_key, None, self.operator)
            
        if self.operator == EqualityOperatorEnum.BETWEEN.value:
            self.locations = self.index.range_scan(self.lower_bound, self.upper_bound_or_key, self.operator)

    def next(self):
        while self.pos < len(self.locations):
            loc = self.locations[self.pos]
            self.pos += 1
            row = self.table._read_row_by_position(*loc)
            record = single_filter_by_tx_snapshot(row)
            
            if record is not None:
                return record

        return None

    def close(self):
        pass
    
class Filter(BaseOperation):
    def __init__(self, operation: TableScan, predicate: "function"):
        self.operation = operation
        self.apply_predicate = predicate
    
    def open(self):
        self.operation.open()
    
    def next(self):
        op_result = self.operation.next()
        while op_result is not None:
            if self.apply_predicate(op_result):
                return op_result
            
            op_result = self.operation.next()
    
    def close(self):
        self.operation.close()
    
class Project(BaseOperation):
    def __init__(self, operation: BaseOperation, column_names: list[str]):
        self.operation = operation
        self.column_names = column_names
        
    def open(self):
        self.operation.open()
        
    def next(self):
        result = self.operation.next()
        
        if result is not None:
            returning = dict()
            
            for column_name in self.column_names:
                if column_name != TOKENS.TIMES:
                    if column_name in result:
                        returning[column_name] = result[column_name]
                    else:
                        raise Exception(f'{column_name} not in column names, try {",".join(self.column_names)}')
                if column_name == TOKENS.TIMES:
                    returning = result
                
            return returning
        
    def close(self):
        self.operation.close()
        
class NestedLoopJoin(BaseOperation):
    def __init__(self, left_op: BaseOperation, right_op: BaseOperation, predicate: "function"):
        self.left_op = left_op
        self.right_op = right_op
        self.predicate = predicate
        self.left_row = None
        self.right_op_opened = False

    def open(self):
        self.left_op.open()
        self.right_op.open()
        self.left_row = None
        self.right_op_opened = True

    def next(self):
        while True:
            # If we don't have a current left row, fetch one
            if self.left_row is None:
                self.left_row = self.left_op.next()
                if self.left_row is None:
                    return None  # no more rows on left
                # reset right for new left row
                
                self.right_op.close()
                self.right_op.open()

            # iterate over right rows
            while True:
                right_row = self.right_op.next()
                if right_row is None:
                    break  # exhausted right rows for current left
                
                if self.predicate(self.left_row, right_row):
                    # merge rows for output
                    new_keys = dict()
                    for key in right_row:
                        if key in self.left_row:
                            new_keys[f'{key}_1'] = right_row[key]
                    
                    result = self.left_row.copy()
                    
                    result.update(new_keys)
                    result.update(right_row)
                    
                    return result

            # finished all right rows for this left row
            self.left_row = None

    def close(self):
        self.left_op.close()
        self.right_op.close()
        

class MultiSort(BaseOperation):
    def __init__(self, operation: BaseOperation, sort_keys: list[tuple[str, bool]]):
        """
        sort_keys: List of tuples [(column_name, reverse), ...]
        reverse = True means descending
        """
        self.operation = operation
        self.sort_keys = sort_keys
        self.sorted_rows = None
        self.pos = 0

    def open(self):
        self.operation.open()
        # Pull all rows into memory
        self.sorted_rows = []
        row = self.operation.next()
        while row is not None:
            self.sorted_rows.append(row)
            row = self.operation.next()

        def sort_key_fn(r):
            return tuple(
                (-r[col] if reverse else r[col]) if isinstance(r[col], (int, float))
                else (r[col] if not reverse else tuple(-ord(c) for c in str(r[col])))
                for col, reverse in self.sort_keys
            )

        self.sorted_rows.sort(key=sort_key_fn)
        self.pos = 0

    def next(self):
        if self.sorted_rows is None:
            raise Exception("MultiSort operator not opened yet")

        if self.pos < len(self.sorted_rows):
            row = self.sorted_rows[self.pos]
            self.pos += 1
            return row
        else:
            return None

    def close(self):
        self.operation.close()
        self.sorted_rows = None
        self.pos = 0
        
class Limit(BaseOperation):
    def __init__(self, operation: BaseOperation, limit: int):
        self.operation = operation
        self.limit = limit
        self.count = 0

    def open(self):
        self.operation.open()
        self.count = 0

    def next(self):
        if self.count >= self.limit:
            return None
        result = self.operation.next()
        if result is not None:
            self.count += 1
        return result

    def close(self):
        self.operation.close()
        
class Aggregate(BaseOperation):
    def __init__(self, operation: BaseOperation, agg_funcs: dict, group_by: list[str] = None):
        """
        operation: upstream operation (TableScan, Filter, etc.)
        agg_funcs: dict mapping column -> aggregate type, e.g., {'salary': 'SUM', 'id': 'COUNT'}
        group_by: list of column names to group by, optional
        """
        self.operation = operation
        self.agg_funcs = agg_funcs
        self.group_by = group_by
        self.result_iter = None

    def open(self):
        self.operation.open()
        self.aggregates = {}

        # Collect all rows and compute aggregates
        while True:
            row = self.operation.next()
            if row is None:
                break

            # Compute group key
            key = tuple(row[col] for col in self.group_by) if self.group_by else None

            if key not in self.aggregates:
                # Initialize aggregation dictionary for this group
                self.aggregates[key] = {col: None for col in self.agg_funcs}
                if 'COUNT' in self.agg_funcs.values():
                    self.aggregates[key]['COUNT'] = 0

            agg_row = self.aggregates[key]

            # Update each aggregate
            for col, func in self.agg_funcs.items():
                val = row[col]

                if func == 'SUM':
                    agg_row[col] = (agg_row[col] or 0) + val
                elif func == 'COUNT':
                    agg_row[col] = (agg_row.get(col, 0) or 0) + 1
                elif func == 'MIN':
                    agg_row[col] = val if agg_row[col] is None else min(agg_row[col], val)
                elif func == 'MAX':
                    agg_row[col] = val if agg_row[col] is None else max(agg_row[col], val)
                elif func == 'AVG':
                    # store sum/count temporarily for AVG
                    if agg_row[col] is None:
                        agg_row[col] = {'sum': val, 'count': 1}
                    else:
                        agg_row[col]['sum'] += val
                        agg_row[col]['count'] += 1
                else:
                    raise Exception(f"Unknown aggregate function: {func}")

        # Prepare final results
        final_results = []
        for key, agg_row in self.aggregates.items():
            result = {}

            # Include group by columns
            if key is not None:
                for i, col_name in enumerate(self.group_by):
                    result[col_name] = key[i]

            # Finalize aggregate values
            for col, func in self.agg_funcs.items():
                if func == 'AVG':
                    s = agg_row[col]['sum']
                    c = agg_row[col]['count']
                    result[col] = s / c if c != 0 else None
                else:
                    result[col] = agg_row[col]

            final_results.append(result)

        self.result_iter = iter(final_results)

    def next(self):
        if self.result_iter:
            return next(self.result_iter, None)
        return None

    def close(self):
        self.operation.close()