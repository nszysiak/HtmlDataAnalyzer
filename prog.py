from FileReader import ZipFileReader
import config
from CreateDir import CreateDirectory


def main():
    file = ZipFileReader(config.ARCHIVE_FILE_PATH + config.FILE_NAME)
    if file.is_7zfile(config.ARCHIVE_FILE_PATH + config.FILE_NAME):
        file.extract_files(config.ARCHIVE_FILE_PATH)
    make_main_dir = CreateDirectory(config.MAIN_DIR_PATH)
    make_main_dir.create_directory()
    make_clicks_dir = CreateDirectory(config.CLICK_ANALYSIS_DIR)
    make_clicks_dir.create_directory()
    make_clicks_dir = CreateDirectory(config.BIZ_ANALYSIS_DIR)
    make_clicks_dir.create_directory()


if __name__ == '__main__':
    main()