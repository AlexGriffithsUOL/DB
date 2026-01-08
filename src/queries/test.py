class Query:
    def open(self):
        raise NotImplementedError()
    
    def next(self):
        raise NotImplementedError()
    
    def close(self):
        raise NotImplementedError()

class TableScan(Query):
    def __init__(self, table):
        self.table = table
        
    def run(self):
        return self.table.scan_all_records()
    
class Filter(Query):
    def __init__(self, predicate):
        print()
        
def main():
    return
        
if __name__ == '__main__':
    main() 