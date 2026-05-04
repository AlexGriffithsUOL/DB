import enum

class TOKENS(enum.Enum):
    SELECT = 'SELECT'
    FROM = 'FROM'
    WHERE = 'WHERE'
    INSERT = 'INSERT'
    INTO = 'INTO'
    VALUES = 'VALUES'
    UPDATE = 'UPDATE'
    SET = 'SET'
    DELETE = 'DELETE'
    CREATE = 'CREATE'
    TABLE = 'TABLE'
    DROP = 'DROP'
    ALTER = 'ALTER'
    ADD = 'ADD'
    JOIN = 'JOIN'
    ON = 'ON'
    GROUP = 'GROUP'
    BY = 'BY'
    HAVING = 'HAVING'
    ORDER = 'ORDER'
    ASC = 'ASC'
    DESC = 'DESC'
    DISTINCT = 'DISTINCT'
    AND = 'AND'
    OR = 'OR'
    NOT = 'NOT'
    NULL = 'NULL'
    IS = 'IS'
    IN = 'IN'
    EXISTS = 'EXISTS'
    BETWEEN = 'BETWEEN'
    LIKE = 'LIKE'

    # Identifiers and literals
    IDENTIFIER = 'IDENTIFIER'
    STRING = 'STRING'
    NUMBER = 'NUMBER'

    # Operators
    EQ = 'EQ'
    NEQ = 'NEQ'
    LT = 'LT'
    LE = 'LE'
    GT = 'GT'
    GE = 'GE'
    PLUS = 'PLUS'
    MINUS = 'MINUS'
    TIMES = 'TIMES'
    DIVIDE = 'DIVIDE'

    # Punctuation
    LPAREN = 'LPAREN'
    RPAREN = 'RPAREN'
    COMMA = 'COMMA'
    SEMICOLON = 'SEMICOLON'
    DOT = 'DOT'
    
    @classmethod
    def keyword_token_map(cls, value):
        mapping = {
            'SELECT' : cls.SELECT,
            'FROM' : cls.FROM,
            'WHERE' : cls.WHERE,
            'INSERT' : cls.INSERT,
            'INTO' : cls.INTO,
            'VALUES' : cls.VALUES,
            'UPDATE' : cls.UPDATE,
            'SET' : cls.SET,
            'DELETE' : cls.DELETE,
            'CREATE' : cls.CREATE,
            'TABLE' : cls.TABLE,
            'DROP' : cls.DROP,
            'ALTER' : cls.ALTER,
            'ADD' : cls.ADD,
            'JOIN' : cls.JOIN,
            'ON' : cls.ON,
            'GROUP' : cls.GROUP,
            'BY' : cls.BY,
            'HAVING' : cls.HAVING,
            'ORDER' : cls.ORDER,
            'ASC' : cls.ASC,
            'DESC' : cls.DESC,
            'DISTINCT' : cls.DISTINCT,
            'AND' : cls.AND,
            'OR' : cls.OR,
            'NOT' : cls.NOT,
            'NULL' : cls.NULL,
            'IS' : cls.IS,
            'IN' : cls.IN,
            'EXISTS' : cls.EXISTS,
            'BETWEEN' : cls.BETWEEN,
            'LIKE' : cls.LIKE,
        }
        if value.upper() in mapping:
            return mapping[value.upper()]
        
        else:
            return cls.IDENTIFIER
    
    # @classmethod
    # def nonalphabetical(cls):
        # return (cls.)
    
class Token:
    def __init__(self, token_type: TOKENS, value, line_no: int, column_no: int):
        self.token_type: TOKENS = token_type
        self.value = value
        self.line_no = line_no
        self.column_no = column_no
        
    def __repr__(self):
        return f'{self.token_type}({self.value}, {self.line_no}, {self.column_no})'