from .logical_nodes import Filter, IndexScan, TableScan
from src.table_manager.manager import TableManager

def optimize(plan, table_manager: TableManager):
    if isinstance(plan, Filter):
        if plan.condition.op == "=":
            col = plan.condition.left
            if table_manager.table_column_has_index(plan.source.table.name, col):
                plan.source = IndexScan(
                    plan.source.table,
                    table_manager.get_index_by_table_col(plan.source.table.name, col),
                    plan.condition
                )
            else:
                plan.source = TableScan(
                    table=plan.source.table
                )
    if hasattr(plan, 'source'):
        plan.source = optimize(plan.source, table_manager)
    return plan