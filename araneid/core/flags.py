from enum import Flag, auto

class Idle(Flag):
    SLOT = auto()
    DOWNLOADERMANAGER = auto()
    SLOTMANAGER = auto()
    SIGNALMANAGER = auto()
    SCHEDULEMANAGER = auto()