from .ast_nodes import SelectStmt, ColumnExpr, BinaryExpr, LiteralExpr
from src.operators.classes import Filter, Project, TableScan, IndexScan
from .tokens import TOKENS
from src.transactions.manager import Transaction

class PlanBuilder:
    def __init__(self, storage_engine):
        self.engine = storage_engine

    def build(self, ast, tx: Transaction):
        if isinstance(ast, SelectStmt):
            return self.build_select(ast, tx)
        else:
            raise NotImplementedError(f"Unsupported AST node: {type(ast)}")

    def build_select(self, stmt: SelectStmt, tx: Transaction):
        scan = self.choose_scan(stmt, tx)

        if stmt.where:
            filter_op = Filter(scan, predicate=self.build_predicate(stmt.where))
        else:
            filter_op = scan

        #  wrap column projection
        project_op = Project(filter_op, column_names=[col.column_name for col in stmt.columns])

        return project_op
    
    def build_predicate(self, expr):
        if isinstance(expr, BinaryExpr):
            left = self.build_predicate(expr.left)
            right = self.build_predicate(expr.right)
            op = expr.op

            def predicate(row):
                lval = left(row)
                rval = right(row)
                if op == TOKENS.EQ: return lval == rval
                if op == TOKENS.GT: return lval > rval
                if op == TOKENS.GE: return lval >= rval
                if op == TOKENS.LT: return lval < rval
                if op == TOKENS.LE: return lval <= rval
                if op == TOKENS.AND: return lval and rval
                if op == TOKENS.OR: return lval or rval
                raise NotImplementedError(f"Operator {op}")

            return predicate

        elif isinstance(expr, ColumnExpr):
            return lambda row: row[expr.column_name]

        elif isinstance(expr, LiteralExpr):
            return lambda row: expr.value

        else:
            raise NotImplementedError(f"Expr {type(expr)} not supported")
        
    def choose_scan(self, stmt: SelectStmt, tx: Transaction):
        # if stmt.where:
            # indexed_column = self.find_indexed_column(stmt.where)
            # if indexed_column:
                # e.g., age > 30
                # expr = self.extract_comparison(stmt.where, indexed_column)
                # return IndexScan(self.engine.get_index(indexed_column), expr)
        # fallback
        return  TableScan(self.engine.get_table(stmt.table), tx)