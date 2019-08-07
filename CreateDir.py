import config
import os


class CreateDirectory:

    def __init__(self, dir_path):
        self.dir_path = dir_path

    def create_directory(self):

        """Function allows to create directory for output files"""

        try:
            os.mkdir(self.dir_path)
        except OSError:
            print("Directory %s has not been created" % self.dir_path)
        else:
            print("Successfully created the dir %s " % self.dir_path)
