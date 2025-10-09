import socket
import subprocess
import os
import logging
from multiprocessing import Process
import clr



# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# These paths are specific to a Windows environment where Hamilton software is installed.
OEM_RUN_EXE_PATH = 'C:\\Program Files (x86)\\HAMILTON\\Bin\\HxRun.exe'
OEM_HSL_PATH = os.path.abspath('VENUS_Method\\STAR_OEM_noFan.hsl')
OEM_STAR_PATH = "star-oem"

clr.AddReference(os.path.join(OEM_STAR_PATH, 'RunHSLExecutor'))
clr.AddReference(os.path.join(OEM_STAR_PATH, 'HSLHttp'))
from RunHSLExecutor import Class1

def terminate_process(process_name):
    """Terminates a process by name."""
    try:
        subprocess.run(['taskkill', '/F', '/IM', process_name], check=True, capture_output=True, text=True)
        logging.info(f"Successfully terminated process {process_name}")
    except FileNotFoundError:
        logging.error("`taskkill` command not found. Are you running on Windows?")
    except subprocess.CalledProcessError as e:
        if "not found" in e.stderr:
            logging.info(f"Process {process_name} not found, no need to terminate.")
        else:
            logging.error(f"Failed to terminate process {process_name}: {e.stderr}")


def main():
    """Starts the TCP server to listen for commands."""
    host = '0.0.0.0'
    port = 65432

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen()
    logging.info(f"Server listening on {host}:{port}")
    #Class1 is the HxExecutor class from RunHSLExecutor.dll
    HxExecutor = Class1()
    while True:
        conn, addr = server_socket.accept()
        with conn:
            logging.info(f"Connected by {addr}")
            data = conn.recv(1024)
            if data.decode().strip() == 'start':
                process_name = os.path.basename(OEM_RUN_EXE_PATH)
                logging.info(f"Attempting to terminate {process_name}...")
                terminate_process(process_name)

                logging.info(f"Starting {OEM_RUN_EXE_PATH} with {OEM_HSL_PATH}...")
                try:
                    HxExecutor.StartMethod(OEM_HSL_PATH)
                    logging.info(f"RunHslExecutor started method {os.path.basename(OEM_HSL_PATH)} successfully.")
                except Exception as e:
                    logging.error(f"Failed to start process: {e}")
            elif data.decode().strip() == 'stop':
                try:
                    HxExecutor.AbortMethod(None)
                    logging.info("AbortMethod called successfully.")
                except Exception as e:
                    logging.error(f"Failed to abort method: {e}")
            else:
                logging.warning(f"Received unknown command: {data.decode()}")

if __name__ == "__main__":
    if not os.path.exists(OEM_HSL_PATH):
        logging.error(f"HSL file not found at {OEM_HSL_PATH}")
    main()
