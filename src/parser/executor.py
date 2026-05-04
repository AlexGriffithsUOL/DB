from src.parser.ast_nodes import SelectStmt, LiteralExpr, BinaryExpr, ColumnExpr
from src.parser.tokens import TOKENS
from src.transactions.manager import Transaction

class Executor:
    def __init__(self, storage_engine):
        self.engine = storage_engine

    def execute(self, ast, tx):
        if isinstance(ast, SelectStmt):
            return self.execute_select(ast, tx)
        else:
            raise NotImplementedError(f"Cannot execute {type(ast)}")

    def execute_select(self, stmt: SelectStmt, tx: Transaction):
        cursor = self.engine.open_cursor(stmt.table, tx)

        results = []
        for row in cursor:
            if stmt.where is None or self.eval_expr(stmt.where, row):
                if stmt.columns[0].name == "*":
                    results.append(row)
                    
                else:
                    projected = {col.name: row[col.name] for col in stmt.columns}
                    results.append(projected)
        return results

    def eval_expr(self, expr, row):
        # recursively evaluate expressions
        if isinstance(expr, LiteralExpr):
            return expr.value
        elif isinstance(expr, ColumnExpr):
            return row[expr.name]
        elif isinstance(expr, BinaryExpr):
            left = self.eval_expr(expr.left, row)
            right = self.eval_expr(expr.right, row)
            if expr.op == TOKENS.EQ:
                return left == right
            elif expr.op == TOKENS.GT:
                return left > right
            elif expr.op == TOKENS.GE:
                return left >= right
            elif expr.op == TOKENS.LT:
                return left < right
            elif expr.op == TOKENS.LE:
                return left <= right
            elif expr.op == TOKENS.AND:
                return left and right
            elif expr.op == TOKENS.OR:
                return left or right
            else:
                raise NotImplementedError(f"Operator {expr.op} not implemented")
        else:
            raise NotImplementedError(f"Expression {type(expr)} not implemented")