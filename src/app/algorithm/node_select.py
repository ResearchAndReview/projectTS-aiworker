"""
!READ ME BEFORE ENTERING DEEP!

현재 메소드의 PARAMETER가 비어 있습니다. 뭔가 필요하다 싶으시면 넣어야 합니다.
현재 주어진 것이 전부가 아닙니다. 더 추가할 수 있으므로 꼭 이야기하시거나 직접 추가해주시면 됩니다.

"""
from src.app.db.mysql import get_available_nodes


def select_node_for_ocr():
    nodes = get_available_nodes()
    if len(nodes) == 0:
        return None
    return nodes[0]


def select_node_for_trans():
    nodes = get_available_nodes()
    if len(nodes) == 0:
        return None
    return nodes[0]
