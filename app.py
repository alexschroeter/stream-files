from arkitekt_next import register, easy
import time
import logging
import argparse
import os
import re
import yaml
from types import SimpleNamespace
from mikro_next.api.schema import (
    Image, # Do we upload the Image or the File?
    from_array_like,
)

class Config:
    def __init__(self, **kwargs):
        self.verbose = kwargs.get('verbose', False)
        self.directory = kwargs.get('directory', ".")
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

        parser = argparse.ArgumentParser(description='Stream files from a directory to Mikro')
        parser.add_argument('--folder', '-f', help='Directory to watch for files')
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

        self.log.debug("The following Arguments have been passed as arguments:")
        for arg in self.args.__dict__:
            self.log.debug(f"{arg}: {self.args.__dict__[arg]}")
        self.log.debug("The following Arguments have been passed by config file:")
        for arg in self.config.__dict__:
            self.log.debug(f"{arg}: {self.config.__dict__[arg]}")

        self.files_already_uploaded = []

        self.app = easy("StreamFilesTest", url=self.config.arkitekt_server)

    def __del__(self):      
        pass        

    def read_config(self):
        self.config = yaml.safe_load(open(self.args.config))

    def read_arguments(self):
        pass

    def get_uploaded_files(self):
        # ToDo - Implement a method to get already uploaded files
        return self.files_already_uploaded

    def upload_image(self, file_location: str) -> Image:
        return from_array_like(
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
                    for f in os.listdir(self.args.directory) 
                    if os.path.isfile(os.path.join(self.args.directory, f)) 
                    and file_regex.match(f)
                    and f not in files_already_uploaded
                    ]
                self.log.debug(f"Files to upload: {files_to_upload}")
                self.log.debug(f"Files already uploaded: {files_already_uploaded}")
                self.log.debug(f"Files in directory: {[f for f in os.listdir(self.args.directory) if os.path.isfile(os.path.join(self.args.directory, f))]}")

                if not files_to_upload:
                    if self.args.run_once:
                        self.log.info("No new files to upload, exiting.")
                        keep_running = False
                    else:
                        self.log.info("No new files to upload, waiting...")
                        time.sleep(5)
                else:
                    for file_name in files_to_upload:
                        file_path = os.path.join(self.args.directory, file_name)
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
                            self.upload_image(file_location=file_path)
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
