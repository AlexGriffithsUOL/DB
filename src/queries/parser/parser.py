import enum

TEST_CONTENT = 'select * from system_indexes;'

class ParserToken(enum.Enum):
    WORD = enum.auto()

class QueryParser:
    def __init__(self, content):
        self.content = content
        self.cursor = 0
    
    @property
    def end_of_query(self):
        return self.cursor == (len(self.content))
    
    @property
    def current_char(self):
        return self.content[self.cursor]
    
    def advance(self):
        self.cursor += 1
    
    def parse(self):
        while not self.end_of_query:
            self.advance()
        return 'test_val'

def main():
    qp = QueryParser(content=TEST_CONTENT)
    test = qp.parse()
    print(test)
        
if __name__ == '__main__':
    main()