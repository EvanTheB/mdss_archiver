#!/usr/bin/env python36

import argparse
import sys
import os
import subprocess
import functools
from pathlib import Path
import collections
MDSS_DIR = "results/crams"
PROJECT = "wq2"
HERE = os.path.dirname(os.path.abspath(__file__))


class keydefaultdict(collections.defaultdict):
    """default dict but give key to function"""
    # https://stackoverflow.com/a/2912455/3936601
    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        else:
            ret = self[key] = self.default_factory(key)
            return ret


def dmlser(id):
    """get all files in path that are on tape"""
    return set(
        subprocess.run(
            "mdss dmls -l " + id + " | awk '$8 ~ /DUL|OFL/ {print $9}'",
            stdout=subprocess.PIPE,
            shell=True,
            check=True,
            encoding='utf8',
        ).stdout.split('\n')
    )

def dmls_size(id):
    """
    get size of one file
    if this gets used more it needs to be cached
    """
    res = subprocess.run(
            f"mdss ls -l {id}".split(),
            stdout=subprocess.PIPE,
            check=True,
            encoding='utf8',
        ).stdout.strip().split('\n')
    assert len(res) == 1, res
    return int(res[0].split()[4])


def dmls_ontape(path, cache=keydefaultdict(dmlser)):
    """is path on tape? cached"""
    return os.path.basename(path) in cache[os.path.dirname(path)]


class Job(object):
    """This class is messily tied in with main
        Just a handy state store.
    """

    def __init__(self, code, file):
        self.file = file
        self.dest = f"{MDSS_DIR}/{code[0:2]}/{code[2:4]}/{os.path.basename(self.file)}"
        self.tape_done = Path(f"{self.file}.tape.done")

        assert Path(file).exists() or self.tape_done.exists()

        self.add_job(
            'put',
            os.path.join(HERE,
                         'mdss_put.pbs.sh'),
            [
                "-l",
                "wd",
                "-v",
                f"FILESOURCE={file},DESTINATION={self.dest}",
            ],
            [],
        )

    def add_job(self, job_name, job_script, qargs, jargs):
        assert len(jargs) == 0
        vars(self)[job_name] = Path(f"{self.file}.{job_name}")
        vars(self)[job_name + '_done'] = Path(f"{self.file}.{job_name}.done")

        def start_job():
            vars(self)[job_name].touch()
            job_jid = subprocess.run(
                ['qsub'] + qargs + [job_script],
                stdout=subprocess.PIPE,
                check=True,
                encoding="utf8",
                cwd=os.path.join(HERE,
                                 'logs'),
            ).stdout.strip()
            print(job_name, job_jid, self.file)
            vars(self)[job_name].write_text(job_jid)
            return job_jid

        vars(self)['start_' + job_name] = start_job

    def check_tape(self):
        # weird logic, dont want to check size until it is on tape
        # and then only once, to make sure we dont spam the system.
        if dmls_ontape(self.dest):
            assert dmls_size(self.dest) == Path(self.file).stat().st_size
            return True
        return False


def count_jobs():
    return int(
        subprocess.run(
            "qstat -u eb8858 | grep -c copyq || true",
            shell="/bin/bash",
            stdout=subprocess.PIPE,
            check=True
        ).stdout
    )


def main(args):
    """
    it is probably better to put bunches of files in together,
    or even whole directories
    """
    put_running = count_jobs()

    for l in args.infile:
        job = l.strip().split('\t')
        assert len(job) == 1
        job = Job(os.path.basename(job[0])[0:5], job[0])

        if not job.put.exists() and put_running < args.put_lim:
            put_running += 1
            job.start_put()
        elif job.put_done.exists() and not job.tape_done.exists():
            if job.check_tape():
                job.tape_done.touch()
                print('tape', job.dest)
                Path(job.file).unlink()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument(
        'infile',
        type=open,
        help='infile',
        default=sys.stdin,
        nargs='?'
    )

    def add(lim, default=1):
        parser.add_argument(
            f'--{lim}-lim',
            type=int,
            help='{lim} limit',
            default=default
        )

    add('put')

    args = parser.parse_args()

    sys.exit(main(args))
