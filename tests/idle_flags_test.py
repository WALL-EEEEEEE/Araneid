import pytest
from araneid.core.flags import Idle

def test_combo_idle_flag_check():
    test_idle = Idle.DOWNLOADERMANAGER | Idle.SCHEDULEMANAGER
    assert Idle.DOWNLOADERMANAGER in test_idle
    assert Idle.SCHEDULEMANAGER in test_idle
    assert Idle.SIGNALMANAGER not in test_idle
