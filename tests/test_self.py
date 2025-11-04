if __name__ == '__main__':
    import sys
    import os

    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))
    
    from windmill_lib.event_bus import EventBus, Event

    bus = EventBus()

    class TestClass:
        def __init__(self):
            pass

        @bus.on("call_test", static=True)
        def handle_call_test(self):
            print("called true!")
    
    test = TestClass()
    bus.publish("call_test")