from src.queries.parser.ast_nodes import *
from src.queries.parser.optimiser import optimize
from src.table_manager.manager import TableManager

TEST_QUERY = "select test_id, name from test where name = 'owugabooga'"

tm = TableManager()

ast = Select(table=tm.tables['test'], columns = ['test_id', 'name', 'ordinal'], where=BinaryOp('name', '=', 'owugabooga'))

plan = build_plan(ast)

optimised_plan = optimize(plan=plan, table_manager=tm)

results = optimised_plan.next()
print(results)