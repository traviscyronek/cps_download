"""
File:   cps_download.py
Author: Travis Cyronek
Date:   28 May 2018
Purpose:
        To download and rename monthly CPS files. A big annoyance is that the
        BLS has changed the naming convention on the path name for the files
        after December 2009. This script seems to handle these issues well. In
        particular, this script locates the files from

        http://www.nber.org/data/cps_basic.html
        http://www.nber.org/data/cps_progs.html

        and determines which files the local directory doesn't have and
        extracts the .raw and .dct files. Because naming conventions for the
        CPS have also changed, the script also harmonizes the naming of the
        files. One thing to note is that early compression formats (Unix '.Z'
        compression) cannot be handled by native python tools. Here I rely on
        the subprocess module to unpack the files with the 'uncompress'
        terminal command: one may use any technique to do the unpacking, and
        should be the only place this scraper would give you a headache. Some
        of the code has been adapted from Tom Augspurger's PyCPS module.
        
NOTE (2020-09-15): THIS CODE IS DEPRECATED BECAUSE OF CHANGES TO THE NBER
WEBSITE. PLEASE USE cps_download_v2.py.
"""


# ----------------------- #
#                         #
#       User Inputs       #
#                         #
# ----------------------- #

out_dir_f = '~/cps/files_raw/'
out_dir_d = '~/cps/dicts/'


# ------------------- #
#                     #
#       Imports       #
#                     #
# ------------------- #

from functools import partial
import io
from lxml.html import parse
import os
import re
import requests
import subprocess
import sys
import urllib.request
import zipfile


# --------------------- #
#                       #
#       Functions       #
#                       #
# --------------------- #

def matcher(link, regex):

    """
    Description: used by python iterator to construct all possible file paths

    Inputs:
    link  -- (str)   name of file to be downloaded
    regex -- (regex) regular expression object

    Notes: the regex's in the truth assessments below must match those in the downloader()
    """

    try:
        if regex == re.compile(r'cpsb\d{4}.Z|\w{3}\d{2}pub.zip'):
            _, _, file_temp = link[2].split('/')
            if regex.match(file_temp):
                return file_temp
        elif regex == re.compile(r'\w*.dct'):
            _, _, _, _, file_temp = link[2].split('/')
            if regex.match(file_temp):
                return file_temp
    except ValueError:
        pass


def exister(path_name):

    """
    Description: determines whether or not a particular month has already been downloaded

    Inputs:
    path_name -- (str) path to check
    """

    del_ext = path_name.split('.')[0] # deletes extension from path_name
    if os.path.exists(path_name) or os.path.exists(del_ext+'.cps') or os.path.exists(del_ext+'.dat') \
            or os.path.exists(del_ext+'.raw') or os.path.exists(del_ext+'.dct'):
        return True


def downloader(out_dir, what_to_dl):

    """
    Description: downloads extracted cps monthly files

    Inputs:
    out_dir    -- (str) place to save downloaded data
    what_to_dl -- (str) {'data', 'dictionaries'}
    """

    # ----- step 1: determine what we want to download ----- #

    if what_to_dl == 'data':
        site   = 'http://www.nber.org/data/cps_basic.html'
        dl_loc = 'http://www.nber.org/data/cps-basic/'
        regex  = re.compile(r'cpsb\d{4}.Z|\w{3}\d{2}pub.zip')

    elif what_to_dl == 'dicts':
        site   = 'http://www.nber.org/data/cps_progs.html'
        dl_loc = 'http://www.nber.org/data/progs/cps/'
        regex  = re.compile(r'\w*.dct')

    else:
        raise(ValueError('Not a valid download input!'))


    # ----- step 2: go to the website find all filetypes that meet our desires ----- #

    parsed = parse(urllib.request.urlopen(site))
    root   = parsed.getroot()
    partial_matcher = partial(matcher, regex=regex)


    # ----- step 3: download what we want if we don't have it already ----- #

    if what_to_dl == 'data':
        m_to_yr = {"jan":"01","feb":"02","mar":"03","apr":"04","may":"05","jun":"06",
                   "jul":"07","aug":"08","sep":"09","oct":"10","nov":"11","dec":"12"}
        for link in filter(partial_matcher,root.iterlinks()):
            _,_,fname_temp,_ = link
            fname = fname_temp.split('/')[-1]
            if fname.startswith(tuple(m_to_yr.keys())):
                fname_alt_1 = 'cpsb'+fname[slice(3,5)]+m_to_yr[fname[:3]]
                year  = float('20'+fname[3:5])
                if year >= 2076:
                    fname_alt_2 = 'cpsb'+'19'+fname[3:5]+m_to_yr[fname[:3]]
                else:
                    fname_alt_2 = 'cpsb'+str(year).split('.')[0]+m_to_yr[fname[:3]]
            else:
                fname_alt_1 = fname
                year = float('20'+fname[4:6])
                if year >= 2076:
                    fname_alt_2 = 'cpsb'+'19'+fname[4:6]+fname[6:8]
                else:
                    fname_alt_2 = 'cpsb'+str(year).split('.')[0]+fname[6:8]
            existing_1 = exister(os.path.join(out_dir,fname))
            existing_2 = exister(os.path.join(out_dir,fname_alt_1))
            existing_3 = exister(os.path.join(out_dir,fname_alt_2))
            if not existing_1 and not existing_2 and not existing_3:
                try:
                    r = requests.get(dl_loc+fname)
                    z = zipfile.ZipFile(io.BytesIO(r.content))
                    z.extractall(out_dir)
                    print('downloaded data: {}'.format(fname))
                except zipfile.BadZipFile:
                    try:
                        open(out_dir+fname,'wb').write(r.content)
                        subprocess.call(['uncompress',os.path.join(out_dir,fname)])
                        os.rename(os.path.join(out_dir,fname.split('.')[0]),out_dir+fname.split('.')[0]+'.dat')
                        print('downloaded data: {}'.format(fname))
                    except:
                        pass
                    pass
        print('>>>>> Data Download Complete! <<<<<')

    elif what_to_dl == 'dicts':
        for link in filter(partial_matcher,root.iterlinks()):
            _,_,fname_temp,_ = link
            fname = fname_temp.split('/')[-1]
            existing = exister(os.path.join(out_dir,fname))
            if not existing:
                r = requests.get(dl_loc+fname)
                with open(out_dir+fname, 'w') as f:
                    f.write(r.text)
                    print('downloaded dictionary: {}'.format(fname))
        print('>>>>> Dictionary Download Complete! <<<<<')


def renamer(out_dir):

    """
    Description: harmonizes names of all data files in a given directory

    Inputs:
    out_dir -- (str) place to save downloaded data
    """

    m_to_yr = {"jan":"01","feb":"02","mar":"03","apr":"04","may":"05","jun":"06",
               "jul":"07","aug":"08","sep":"09","oct":"10","nov":"11","dec":"12"}
    files_to_rename = os.listdir(out_dir)[1:]
    for files in files_to_rename:
        fname,ext = files.split('.')
        if fname.startswith(tuple(m_to_yr.keys())):
            if ext == 'dat':
                os.rename(out_dir+files,str(out_dir)+'cpsb'+fname[slice(3,5)]+m_to_yr[fname[:3]]+'.'+ext)
            else:
                os.rename(out_dir+files,str(out_dir)+'cpsb'+fname[slice(3,5)]+m_to_yr[fname[:3]]+'.dat')
    files_to_rename = os.listdir(out_dir)[1:]
    for files in files_to_rename:
        fname,ext = files.split('.')
        if ext == 'raw':
            pass
        else:
            year  = float('20'+fname[4:6])
            if year >= 2076: # this 'technique' will cease to work in 2076
                os.rename(out_dir+files,str(out_dir)+'cpsb'+'19'+fname[4:6]+fname[6:8]+'.raw')
            else:
                os.rename(out_dir+files,str(out_dir)+'cpsb'+str(year).split('.')[0]+fname[6:8]+'.raw')


# -------------------- #
#                      #
#       Download       #
#                      #
# -------------------- #

if __name__ == '__main__':
    downloader(out_dir_f, 'data')
    renamer(out_dir_f)
    downloader(out_dir_d, 'dicts')
