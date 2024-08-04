from asyncio import Event
from enum import IntFlag, auto


class States(IntFlag):
    pass

class SpiderStates(States):
    STARTER_CLOSE = auto()
    PARSER_CLOSE = auto()
    CLOSE = auto()
    RUNNING = auto()
    START = auto()



class State(object):
    def __init__(self) -> None:
        self.__states__ = {}
        for state in  self.States:
            self.__states__[state] = Event()

    def in_state(self, state: States):
        assert state in self.States 
        state: Event = self.__states__.get(state)
        return state.is_set()

    def get_state(self, state: States) -> Event:
        assert state in  self.States
        return self.__states__.get(state)
    
    async def wait_state(self, state: States) -> None:
        assert state in self.States
        state: Event = self.__states__.get(state)
        await state.wait()
    
    def set_state(self, state: States):
        assert state in self.States 
        state: Event =  self.__states__.get(state)
        if not state.is_set():
           state.set()
    
    def reset_state(self, state:States):
        assert state in self.States
        state: Event = self.__states__.get(state)
        if state.is_set():
           state.clear()
    
    def clear_states(self):
        for state  in self.__states__.values():
            state :Event = state
            state.clear()

class SpiderState(State):
    States = SpiderStates


