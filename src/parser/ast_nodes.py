from .tokens import Token, TOKENS

class AbstractASTBaseNode:
    pass

# Expressions
class Expr(AbstractASTBaseNode):
    pass

class ColumnExpr(Expr):
    def __init__(self, column_name: str):
        self.column_name = column_name
        
class LiteralExpr(Expr):
    def __init__(self, value: str):
        self.value = value

class BinaryExpr(AbstractASTBaseNode):
    def __init__(self, left: Expr, op: Token, right: Expr):
        self.left: Expr = left
        self.op: Token = op
        self.right: Expr = right

# Statements
class SelectStmt(AbstractASTBaseNode):
    def __init__(self, columns, table, where=None):
        self.columns = columns       # List[Expr]
        self.table = table           # str
        self.where = where           # Expr or None
# SelectStmt* parseSelect() {
#     expect(TokenType::SELECT); // consume SELECT token

#     auto columns = parseColumnList(); // a helper

#     expect(TokenType::FROM);

#     std::string tableName = expect(TokenType::IDENTIFIER).value;

#     Expr* where = nullptr;
#     if (match(TokenType::WHERE)) {
#         where = parseExpression();
#     }

#     return new SelectStmt{columns, tableName, where};
# }