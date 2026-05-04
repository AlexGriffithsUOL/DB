from typing import Any
from .tokens import TOKENS, Token

class Tokeniser:
    def __init__(self, content: list[str]):
        if isinstance(content, str):
            content = content.split('\n')
            
        self.content = content
        self.content_character_length = sum([len(l) for l in self.content])
        
        
    @property
    def line(self):
        return self.content[self.line_number]
    
    @property
    def cursor(self) -> str:
        return self.line[self.column_number]
    
    @property
    def at_end_of_content(self):
        return self.line_number == len(self.content) - 1
    
    @property
    def at_end_of_line(self):
        return self.column_number == len(self.line)
    
    @property
    def at_end_of_file(self):
        return self.at_end_of_content and self.at_end_of_line
    
    @property
    def next_char(self):
        return self.content[self.column_number + 1]
    
    @property
    def cursor_at_valid_character(self):
        if self.at_end_of_line:
            return False
        
        return not self.cursor in ('', ' ', '\n')
    
    def increment_cursor(self):
        if not self.at_end_of_line:
            self.column_number += 1
        
        if self.at_end_of_line:
            self.column_number = 0
            
            if not self.at_end_of_content:
                self.line_number += 1
                
    def skip_whitespace(self):
        while not self.cursor_at_valid_character:
            self.increment_cursor()
    
    def lex_alphanum(self):
        word = ''
        while not self.at_end_of_line:
            while self.cursor_at_valid_character:
                if self.is_alphanumeric(self.cursor):
                    word += self.cursor
                    self.column_number += 1
                else:
                    break
                
            return word
        
    def generate_token(self, token_type: TOKENS, value: Any) -> Token:
        return Token(token_type, value, self.line_number, self.column_number)
        
    def is_alphanumeric(self, value: str):
        return value.isalnum() or value == '_'

    def lex(self):
        word = ''
        counter = 0
        
        while not self.at_end_of_file:
            while self.cursor_at_valid_character:
                if counter > self.content_character_length:
                    raise Exception('Unknown error occurred')
                
                if self.cursor == "'":
                    self.column_number += 1
                    
                    word = self.lex_alphanum()
                    
                    if self.cursor == "'":
                        self.column_number += 1
                        return self.generate_token(TOKENS.STRING, word)
                    
                if self.cursor == ',':
                    word += self.cursor
                    self.column_number += 1
                    return self.generate_token(TOKENS.COMMA, word)
                
                if self.cursor == '.':
                    word += self.cursor
                    token = self.generate_token(TOKENS.DOT, word)
                    self.column_number += 1
                    return token
                
                if self.cursor == '*':
                    word += self.cursor
                    token = self.generate_token(TOKENS.TIMES, word)
                    self.column_number += 1
                    return token
                
                if self.cursor == '=':
                    word += self.cursor
                    token = self.generate_token(TOKENS.EQ, word)
                    self.column_number += 1
                    return token
                
                if self.cursor == '>':
                    word += self.cursor
                    token = token = self.generate_token(TOKENS.GT, word)
                    self.column_number += 1
                    
                    if self.cursor == '=':
                        word += self.cursor
                        token = self.generate_token(TOKENS.GE, word)
                        self.column_number += 1
                        return token
                        
                    return token
                
                if self.cursor == '<':
                    word += self.cursor
                    token = self.generate_token(TOKENS.LT, word)
                    self.column_number += 1
                    
                    
                    if self.cursor == '=':
                        word += self.cursor
                        token = self.generate_token(TOKENS.LE, word)
                        self.column_number += 1
                        return token
                        
                    return token
                
                if self.cursor == ';':
                    word += self.cursor
                    token = self.generate_token(TOKENS.SEMICOLON, word)
                    self.column_number += 1
                    return token
                
                if self.is_alphanumeric(self.cursor):
                    word = self.lex_alphanum()
                    
                    if word.isnumeric() or word.isdecimal():
                        return self.generate_token(TOKENS.NUMBER, word)
                    
                    token_type = TOKENS.keyword_token_map(word)
                    
                    return self.generate_token(token_type, word)
                
                counter += 1
            
            return word
        
    def tokenise(self):
        # try:
            tokens = []
        
            self.line_number = 0
            self.column_number = 0
            
            while not self.at_end_of_file:
                self.skip_whitespace()
                
                token = self.lex()
                tokens.append(token)
                
            return tokens