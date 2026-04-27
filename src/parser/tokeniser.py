import ply.lex as lex
from src.parser.tokens import TOKENS as tokens
from src.parser.regex import (
    t_EQ,
    t_NEQ,
    t_LT,
    t_LE,
    t_GT,
    t_GE,
    t_PLUS,
    t_MINUS,
    t_TIMES,
    t_DIVIDE,
    t_LPAREN,
    t_RPAREN,
    t_COMMA,
    t_SEMICOLON,
    t_DOT
)
from src.parser.identifiers import (
    t_ignore,
    t_newline,
    t_IDENTIFIER,
    t_NUMBER,
    t_STRING,
    t_COMMENT,
    t_error
)

class Tokeniser:
    def __init__(self):
        self.lexer = lex.lex()
        
    def tokenise(self, input):
        self.lexer.input(input)
        token_list = []
        
        while True:
            tok = self.lexer.token()
            
            if tok:
                token_list.append(tok)
                
            if not tok:
                return token_list