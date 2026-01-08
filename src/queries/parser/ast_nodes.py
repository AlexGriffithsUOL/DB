from .logical_nodes import *

class ASTNode:
    """The base AST Node class type for easier interpretation"""
    pass

class Select(ASTNode):
    """The basic Select AST Node"""
    
    def __init__(self, table, columns, where=None):
        self.table = table
        self.columns = columns
        self.where = where
        
    def __repr__(self):
        return f'Select({self.table}, {self.columns}, {self.where})'

class BinaryOp:
    def __init__(self, left, op, right):
        self.left = left
        self.op = op
        self.right = right
    
    def __repr__(self):
        return f'BinaryOp({self.left} {self.op} {self.right})'
    
    def eval_record(self, record):
        match (self.op):
            case '<':
                return record[self.left] < self.right
            case '>':
                return record[self.left] > self.right
            case '=':
                return record[self.left] == self.right

def build_plan(ast):
    scan = Scan(ast.table)

    node = scan
    if ast.where:
        node = Filter(ast.where, node)

    node = Project(ast.columns, node)
    return node

def __main__():
    Select(table='system_indexes', where=BinaryOp('name', '=', 'owugabooga'))