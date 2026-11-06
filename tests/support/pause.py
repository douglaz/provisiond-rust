"""Deterministic simulator-only acceptance/reply points; no external service calls."""
import threading


class Pause:
    def __init__(self):
        self.reached = threading.Event()
        self.release = threading.Event()

    def arrive(self):
        self.reached.set()
        assert self.release.wait(10), 'simulator pause was never released'


class Simulator:
    def __init__(self):
        self.acceptance = Pause()
        self.reply = Pause()
        self.accepted = False

    def call(self):
        self.acceptance.arrive()
        self.accepted = True
        self.reply.arrive()
        return 'simulated reply'
