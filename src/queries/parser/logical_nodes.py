class LogicNode:
    pass

class Scan(LogicNode):
    def __init__(self, table):
        self.table = table
        
    def __repr__(self):
        return f'Scan("{self.table}")'

class Filter(LogicNode):
    def __init__(self, condition, source):
        self.condition = condition
        self.source = source
        
    def filter(self, records):
        return [x for x in records if self.condition.eval_record(x)]
        
    def next(self):
        while True:
            record = self.source.next()
            if record is None:
                return None
            return self.filter(record)
        
    def __repr__(self):
        return f'Filter({self.condition}, {self.source})'

class Project(LogicNode):
    def __init__(self, columns, source):
        self.columns = columns
        self.source = source
        
    def next(self):
        records = self.source.next()
        
        if records is None:
            return None

        new_dict = []
        
        for record in records:
            temp = dict()
            for col in self.columns:
                temp[col] = record[col]
            new_dict.append(temp)
            
            
        return new_dict
        
    def __repr__(self):
        return f'Project({self.columns}, {self.source})'


class TableScan(Scan):
    def __init__(self, table):
        super().__init__(table=table)
    
    def next(self):
        return self.table.scan_all_records()
    
    def __repr__(self):
        return f'Table{super().__repr__()}'

class IndexScan(Scan):
    def __init__(self, table, index, condition):
        super().__init__(table)
        self.index = index
        self.condition = condition
        
    def next(self):
        return
        
    def __repr__(self):
        return f'IndexScan({self.table}, {self.condition}, {self.index})'