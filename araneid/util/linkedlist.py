class Node:
    def __init__(self, value) -> None:
        self.value = value
        self.prev = None
        self.next = None

class LinkedList:
    def __init__(self) -> None:
        self.head = None
        self.tail = None
    
    def add_after(self, target_node_data: Node, new_node:Node):
        if self.head is None:
            raise Exception("List is empty")

        for node in self:
            if node.value == target_node_data:
                new_node.next = node.next
                node.next = new_node
                return
        raise Exception("Node with data '%s' not found" % target_node_data)
