from configparser import SafeConfigParser
import os

class marvelous_config:

    def __init__(self, filename=None, cdict=None):
        self.config = SafeConfigParser()
        if filename is not None:
            self.filename = filename
        else:
            self.filename = os.path.join('.', 'config.ini')

        if os.path.isfile(self.filename):
            self.config.read(self.filename)
        else:
            self.config.add_section('general')
            self.config.add_section('daligner')

        if cdict is not None:
            self.set_dict(cdict)

        self.save()

    def get(self, section, key, default=None):
        value = self.config.get(section, key, fallback=default)
        if value == 'None':
            return None
        return value

    def getboolean(self, section, key, default=None):
        return self.config.getboolean(section, key, fallback=default)

    def getint(self, section, key, default=None):
        return self.config.getint(section, key, fallback=default)

    def getfloat(self, section, key, default=None):
        return self.config.getfloat(section, key, fallback=default)

    def set_dict(self, cdict):
        for section, c in cdict.items():
            if section not in self.config.sections():
                self.config.add_section(section)
            for key, value in c.items():
                self.config.set(section, key, str(value))

    def set(self, section, key, value):
        self.config.set(section, key, str(value))
        self.save()

    def save(self):
        with open(self.filename, 'w') as f:
            self.config.write(f)
