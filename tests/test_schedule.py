from cookplanner.schedule import Schedule


def test_schedule_keeps_track_of_last_scheduled():
    sched = Schedule()
    sched.update()
