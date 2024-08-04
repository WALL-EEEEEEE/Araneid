from asyncio import Task

class Pipeline(Task):

    def __init__(self, coro):
        super().__init__(coro)

    def set_name(self, name):
        self.name = name
        
    def get_name(self):
        return self.name