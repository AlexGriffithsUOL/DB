from .ast_nodes import SelectStmt, ColumnExpr, BinaryExpr, LiteralExpr
from .tokens import TOKENS

class Parser:
    def __init__(self, tokens):
        self.tokens = tokens
        self.pos = 0

    def peek(self):
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return None

    def advance(self):
        tok = self.peek()
        self.pos += 1
        return tok

    def match(self, *types):
        tok = self.peek()
        if tok and tok.token_type in types:
            self.advance()
            return True
        return False

    def expect(self, type_):
        tok = self.advance()

        if not tok or tok.token_type != type_:
            raise SyntaxError(f"Expected {type_} but got {tok}")
        return tok
    
    def parse_select_list(self):
        columns = []
        if self.match(TOKENS.TIMES):
            columns.append(ColumnExpr(TOKENS.TIMES))
        else:
            while True:
                columns.append(ColumnExpr(self.expect(TOKENS.IDENTIFIER).value))
                if not self.match(TOKENS.COMMA):
                    break
        return columns
    
    def parse_select(self):
        self.expect(TOKENS.SELECT)

        columns = self.parse_select_list()

        self.expect(TOKENS.FROM)
        table = self.expect(TOKENS.IDENTIFIER).value

        where = None
        if self.match(TOKENS.WHERE):
            where = self.parse_expression()

        return SelectStmt(columns, table, where)
    
    def parse_expression(self):
        return self.parse_or()

    def parse_or(self):
        left = self.parse_and()
        while self.match(TOKENS.OR):
            right = self.parse_and()
            left = BinaryExpr(left, TOKENS.OR, right)
        return left

    def parse_and(self):
        left = self.parse_comparison()
        while self.match(TOKENS.AND):
            right = self.parse_comparison()
            left = BinaryExpr(left, TOKENS.AND, right)
        return left

    def parse_comparison(self):
        left = self.parse_term()
        if self.match(TOKENS.EQ, TOKENS.NEQ, TOKENS.LT, TOKENS.LE, TOKENS.GT, TOKENS.GE):
            op = self.tokens[self.pos - 1].token_type
            right = self.parse_term()
            return BinaryExpr(left, op, right)
        return left

    def parse_term(self):
        tok = self.advance()
        if tok.token_type == TOKENS.IDENTIFIER:
            return ColumnExpr(tok.value)
        elif tok.token_type == TOKENS.STRING:
            return LiteralExpr(tok.value)
        elif tok.token_type == TOKENS.NUMBER:
            return LiteralExpr(int(tok.value))
        elif tok.token_type == TOKENS.LPAREN:
            expr = self.parse_expression()
            self.expect(TOKENS.RPAREN)
            return expr
        else:
            raise SyntaxError(f"Unexpected token: {tok}")