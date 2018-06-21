import os
import sqlite3

class marvel_db:

    def __init__(self, filename):
        is_new = not os.path.exists(filename)
        self._db = sqlite3.connect(filename)
        self._c = self._db.cursor()

        if is_new:
            self._c.execute('''CREATE TABLE jobs (name, block1, block2, status,
                      latest_update)''')
            self._db.commit()

    def add_block(self):
        pass
