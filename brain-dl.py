import io
import logging
from multiprocessing import Pool
import os
from functools import partial
import random
import re
import sched
import signal
import time
from tqdm import tqdm

import click
import coloredlogs
import httplib2
from pydrive.auth import AuthenticationError, GoogleAuth, RefreshError
from pydrive.auth import ServiceAccountCredentials as SAC
from pydrive.drive import GoogleDrive

logger = logging.getLogger(__name__)

# log to console
coloredlogs.install(
    logger=logger,
    level='INFO',
    fmt='%(asctime)s  %(levelname)s %(message)s',
    datefmt='%H:%M:%S'
)
# log error to file
err_handler = logging.FileHandler('error.log')
err_handler.setLevel(logging.ERROR)
err_formatter = logging.Formatter('%(asctime)s  %(levelname)s %(message)s')
err_handler.setFormatter(err_formatter)
logger.addHandler(err_handler)

class TqdmToLogger(io.StringIO):
    """
    Output stream for TQDM which will output to logger module instead of the sys.stdout
    """
    logger = None
    level = None
    buf = ''

    def __init__(self, logger, level=logging.INFO):
        super(TqdmToLogger, self).__init__()
        self.logger = logger
        self.level = level

    def write(self, buf):
        self.buf = buf.strip('\r\n\t ')

    def flush(self):
        self.logger.log(self.level, self.buf)

tqdm_out = TqdmToLogger(logger, level=logging.INFO)

def download_file(fo, dst_dir=''):
    fn = fo['title']
    logging.debug("downloading \"{}\"".format(fn))

    fp = os.path.join(dst_dir, fn)
    try:
        fo.GetContentFile(fp)
    finally:
        pause = random.randint(10, 30)
        time.sleep(pause)

def download_file_id(file_id, dst_dir='', n_retries=5):
    fo = drive.CreateFile({'id': file_id})
    try:
        for i in range(n_retries):
            try:
                download_file(fo, dst_dir=dst_dir)
                return
            except ConnectionAbortedError:
                logger.warning("disconnected, retry ({})".format(i))
            except httplib2.ServerNotFoundError:
                logger.warning("throttled, retry ({})".format(i))
            except (AuthenticationError, RefreshError):
                logger.warning("token expired, retry ({})".format(i))
        logger.error("\"{}\", give up after retries".format(file_id))
    except:
        logger.error("\"{}\", unknown exception".format(file_id))

def scan_for_files(drive, file_id, max_return=32):
    logger.info("scanning \"{}\"...".format(file_id))
    paginator = drive.ListFile({
        'q': "'{}' in parents and trashed=false".format(file_id),
        'maxResults': max_return
    })
    for file_list in paginator:
        for fo in file_list:
            # ignore nested folders
            if fo['mimeType'] == 'application/vnd.google-apps.folder':
                continue
            yield fo

def get_folder_name(drive, file_id):
    fo = drive.CreateFile({'id': file_id})
    return fo['title']

def find_file_id(url, pattern=r'https://drive.google.com/drive/folders/(.*)$'):
    token = re.search(pattern, url)
    try:
        return token.group(1)
    except AttributeError:
        raise ValueError("unable to determine file id")

def retrieve_file_ids(drive, file_id, save_as=None):
    logger.info("no known ID list, scanning...")

    # scheduler for progress reminder
    s = sched.scheduler(time.time, time.sleep)
    def action(i, fd):
        fd.flush()
        logger.info("found {} files".format(i))

    # get id list
    file_list = scan_for_files(drive, file_id)
    if save_as:
        with open(save_as, 'w') as fd:
            i = 0
            for fo in file_list:
                fd.write("{}\n".format(fo['id']))
                i += 1
        
                if s.empty():
                    s.enter(5, 1, action, (i, fd))
                s.run(blocking=False)
    else:
        file_ids = []
        for fo in file_list:
            file_ids.append(fo)
            
            if s.empty():
                s.enter(5, 1, action, (i, fd))
            s.run(blocking=False)

        return file_id

@click.command()
@click.argument('url')
@click.argument('dst_dir')
@click.option('-n', '--n_workers', default=12, help='number of greetings')
def download_link(url, dst_dir, n_workers):
    # target
    file_id = find_file_id(url)

    # destinations
    dn = get_folder_name(drive, file_id)
    dst_dir = os.path.join(os.path.expanduser(dst_dir), dn)
    try:
        os.makedirs(dst_dir)
        logger.info("destination \"{}\" created".format(dst_dir))
    except:
        pass
    
    # buffer to file
    file_id_fn = 'id_{}.txt'.format(file_id)
    if not os.path.exists(file_id_fn):
        retrieve_file_ids(drive, file_id, save_as=file_id_fn)

    # load id list
    with open(file_id_fn, 'r') as fd:
        file_ids = [line.strip() for line in fd.readlines()]

    def init_worker():
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    # download
    pool = Pool(n_workers, init_worker)
    try:
        func = partial(download_file_id, dst_dir=dst_dir)
        with tqdm(
            total=len(file_ids), mininterval=5,
            file=tqdm_out
        ) as pbar:
            for _ in tqdm(pool.imap_unordered(func, file_ids)):
                pbar.update()
    except KeyboardInterrupt:
        pass
    finally:
        pool.terminate()
        pool.join()

if __name__ == '__main__':
    gauth = GoogleAuth()

    # method 1 - service account
    #scope = ['https://www.googleapis.com/auth/drive.file']
    #gauth.credentials = SAC.from_json_keyfile_name('client_secret.json', scope)

    # method 2 - oauth
    auth_url = gauth.GetAuthUrl()
    print("Please visit\n\n{}\n\nto retrieve OAuth authorization code.".format(auth_url))
    code = input(".. CODE: ")
    gauth.Auth(code)

    drive = GoogleDrive(gauth)

    download_link()
