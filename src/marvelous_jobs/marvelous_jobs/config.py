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
        if not self.config.has_option(section, key):
            raise KeyError('key {}:{} does not exist'.format(section, key))
        value = self.config.get(section, key, fallback=default)
        if value == 'None':
            return None
        return value

    def getboolean(self, section, key, default=None):
        return self.config.getboolean(section, key, fallback=default)

    def getint(self, section, key, default=None):
        if self.get(section, key, default) is None:
            return None
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
        """Set a config value.

        If `section` does not yet exist, it is created.

        Parameters
        ----------
        section : str
            Name of the section to set the key in.
        key : str
            Name of the key to set the value for.
        value : object
            The value to set for the key. This object is
            saved as `str(value)`.
        """
        if not self.config.has_section(section):
            self.config.add_section(section)
        self.config.set(section, key, str(value))
        self.save()

    def update(self, section, key, value, default=None):
        """Update a config value.

        If `value` is `None` and the key has not been set to
        a non-`None` value already, then the key is set to
        the default value. If `value` is not `None` the key
        will get this value. If neither of these cases apply
        it means the key already has a value and that `value`
        is `None`, and nothing will changed.

        Note that if the section and/or key does not exist,
        it will be created with an inital value of `None`.

        Parameters
        ----------
        section : str
            Name of the section to set the key in.
        key : str
            Name of the key to set the value for.
        value : object
            The value to set for the key. This object is
            saved as `str(value)`.
        default : object, optional
            The value to set if `value` is `None`. This object
            is saved as `str(value)`.
        """
        if not self.config.has_section(section):
            self.config.add_section(section)
        if not self.config.has_option(section, key):
            self.set(section, key, None)
        if value is None and self.get(section, key) is None:
            self.set(section, key, default)
        elif value is not None:
            if self.get(section, key) != value:
                self.set(section, key, value)

    def save(self):
        """Save the config to file."""
        with open(self.filename, 'w') as f:
            self.config.write(f)
