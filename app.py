import time
import logging
import argparse
import os
import re
import yaml

from arkitekt_next import register, easy
from mikro_next.api.schema import (
    File,
    from_file_like,
)

class Config:
    def __init__(self, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        self.folder = kwargs.get('folder', ".")
        self.pattern = kwargs.get('pattern', ".*")
        self.dataset_id = kwargs.get('dataset_id', None)
        self.dataset_name = kwargs.get('dataset_name', 'Streaming Dataset')
        self.run_once = kwargs.get('run_once', False)
        self.config = kwargs.get('config', None)
        self.arkitekt_server = kwargs.get('arkitekt_server', 'go.arkitekt.live')

class FileStreamer:
    """
    A class to handle file streaming.
    """
    def __init__(self):
        self.config = Config()

        parser = argparse.ArgumentParser(description='Stream files from a Folder to Mikro')
        parser.add_argument('--folder', '-f', help='Folder to watch for files')
        parser.add_argument('--pattern', '-p', help='Regular expression to filter files')
        parser.add_argument('--dataset-id', '-d', help='ID of existing dataset to use')
        parser.add_argument('--dataset-name', '-n', default='Streaming Dataset',
                            help='Name for new dataset if dataset-id not provided')
        parser.add_argument('--run-once', '-r', action='store_true', 
                            help='Run once and exit')
        parser.add_argument('--config', '-c', help='Provide a configuration file')
        parser.add_argument('--verbose', action='store_true', 
                            help='Enable debug logging')
        parser.add_argument('--arkitekt-server', '-a', help="Arkitekt Server")
        self.args = parser.parse_args()

        # If a configuration file is provided, we use it to overwrite the default values
        self.config = self.read_config()
        # If other arguments are provided, we overwrite those as well
        self.config = self.read_arguments()

        # Initialize the logging
        logging.basicConfig(
            level=logging.DEBUG if (self.args.verbose or self.config.verbose) else logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        )
        self.log = logging.getLogger(__name__)

        self.log.debug("The following Arguments have been passed by config file:")
        for arg in self.config.__dict__:
            self.log.debug(f"{arg}: {self.config.__dict__[arg]}")

        self.log.debug("The following Arguments have been passed as arguments:")
        for arg in self.args.__dict__:
            self.log.debug(f"{arg}: {self.args.__dict__[arg]}")

        self.files_already_uploaded = []

        self.app = easy("StreamFilesTest", url=self.config.arkitekt_server)
        self.app.register(self.upload_file)

    def __del__(self):      
        pass        

    def read_config(self):
        # if self.args.config:
        #     self.config = yaml.safe_load(open(self.args.config))
        return self.config

    def read_arguments(self):
        return self.config

    def get_uploaded_files(self):
        # ToDo - Implement a method to get already uploaded files
        return self.files_already_uploaded

    def upload_file(self, file_location: str, file_name: str) -> File:
        with open(file_location, mode="r") as f:
            return from_file_like(
                    name=file_name, 
                    file=f,
                )

    def run(self):
        self.log.info("Starting FileStreamer...")
        keep_running = True
        while keep_running:
            try:
                # Get files that match the pattern and haven't been uploaded

                file_regex = re.compile(self.args.pattern) if self.args.pattern else re.compile(".*")
                files_already_uploaded = self.get_uploaded_files()
                files_to_upload = [
                    f 
                    for f in os.listdir(self.args.folder) 
                    if os.path.isfile(os.path.join(self.args.folder, f)) 
                    and file_regex.match(f)
                    and f not in files_already_uploaded
                    ]
                self.log.debug(f"Files to upload: {files_to_upload}")
                self.log.debug(f"Files already uploaded: {files_already_uploaded}")
                self.log.debug(f"Files in folder: {[f for f in os.listdir(self.args.folder) if os.path.isfile(os.path.join(self.args.folder, f))]}")

                if not files_to_upload:
                    if self.args.run_once:
                        self.log.info("No new files to upload, exiting.")
                        keep_running = False
                    else:
                        self.log.info("No new files to upload, waiting...")
                        time.sleep(5)
                else:
                    for file_name in files_to_upload:
                        file_path = os.path.join(self.args.folder, file_name)
                        try:
                            # Test if file is accessible (not being written to)
                            os.rename(file_path, file_path)
                        except OSError:
                            self.log.warning(
                                f"Could not access {file_name}. Probably still in use. Will try again later."
                            )
                            continue
                        
                        self.log.info(f"Uploading: {file_path}")
                        try:
                            self.upload_file(file_location=file_path, file_name=file_name)
                            self.files_already_uploaded.append(file_name)
                            self.log.info(f"Successfully uploaded: {file_path} (ID: {"uploaded_file"})")
                        except Exception as e:
                            self.log.error(f"Failed to upload {file_path}: {str(e)}")
                    
                    time.sleep(1)
                   
            except KeyboardInterrupt:
                self.log.info("Process interrupted by user")
                break
            except Exception as e:
                self.log.error(f"Error in file streaming: {str(e)}")
                if self.run_once:
                    break
                time.sleep(5)  # Wait a bit before retrying in case of errors


if __name__ == "__main__":
    my_FileStreamer = FileStreamer()
    my_FileStreamer.run()
