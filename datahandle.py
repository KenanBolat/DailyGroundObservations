import os
import csv


class Handle:

    def __init__(self):
        pass

    @staticmethod
    def get_file_tags(filename, delimiter="_"):
        file_tag = os.path.splitext(os.path.basename(filename))[0]
        date_tag = file_tag.split(delimiter)
        return date_tag

    @staticmethod
    def _check_file_exists(filename):
        return os.path.exists(filename)

    # @staticmethod
    # def write_data(filename):
        # if self._check_file_exists(filename):
        #     print ("This file already exists ! Filename: ", filename)
        # else:
        #     print ("File : ", filename, "write successful ! ")
