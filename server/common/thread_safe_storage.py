import threading
from .utils import store_bets, load_bets, has_won

class ThreadSafeStorage:
    def __init__(self):
        self.lock = threading.RLock()

    def store_bets(self, bets):
        with self.lock:
            return store_bets(bets)
        
    def load_bets(self):
        with self.lock:
            return load_bets()

    def has_won(self, bet):
        return has_won(bet)
        
storage = ThreadSafeStorage()