from configparser import SafeConfigParser
import os

class marvelous_config:

    def __init__(self, filename, cdict=None):
        self.config = SafeConfigParser()
        self.filename = filename
        if os.path.isfile(filename):
            self.config.read(filename)
        else:
            self.config.add_section('general')
            self.config.add_section('daligner')

        if cdict is not None:
            self.set_dict(cdict)

        self.save()

    def get(self, section, key, default=None):
        return self.config.get(section, key, fallback=default)

    def getboolean(self, section, key, default=None):
        return self.config.getboolean(section, key, fallback=default)

    def getint(self, section, key, default=None):
        return self.config.getint(section, key, fallback=default)

    def set_dict(self, cdict):
        for section, c in cdict.items():
            if section not in self.config.sections():
                self.config.add_section()
            for key, value in c.items():
                self.config.set(section, key, str(value))

    def set(self, section, key, value):
        self.config.set(section, key, str(value))
        self.save()

    def save(self):
        with open(self.filename, 'w') as f:
            self.config.write(f)
