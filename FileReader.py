import py7zlib
import os


class ZipFileReader:

            def __init__(self, file_path):
                fp = open(file_path, 'rb')
                self.archive = py7zlib.Archive7z(fp)

            @classmethod
            def is_7zfile(cls, file_path):
                is_7z = False
                fp = None
                try:
                    fp = open(file_path, 'rb')
                    archive = py7zlib.Archive7z(fp)
                    n = len(archive.getnames())
                    is_7z = True
                finally:
                    if fp:
                        fp.close()
                return is_7z

            def extract_files(self, path):
                for name in self.archive.getnames():
                    out_file_name = os.path.join(path, name)
                    out_dir = os.path.dirname(out_file_name)
                    if not os.path.exists(out_dir):
                        os.makedirs(out_dir)
                    outfile = open(out_file_name, 'wb')
                    outfile.write(self.archive.getmember(name).read())
                    outfile.close()



