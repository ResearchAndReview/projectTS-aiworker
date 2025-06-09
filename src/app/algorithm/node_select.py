from src.app.db.mysql import get_available_nodes

def calc_node_completion_time(node):
    return node['ocr_task_size'] / node['ocr_perf'] + node['trans_task_size'] / node['trans_perf']

def select_node_for_ocr(ocr_task_size: int):
    nodes = get_available_nodes()
    if len(nodes) == 0:
        return None

    nodes.sort(key=lambda node: calc_node_completion_time(node) + ocr_task_size / node['ocr_perf'])
    return nodes[0]

def select_node_for_trans(trans_task_size: int):
    nodes = get_available_nodes()
    if len(nodes) == 0:
        return None

    nodes.sort(key=lambda node: calc_node_completion_time(node) + trans_task_size / node['trans_perf'])
    return nodes[0]
