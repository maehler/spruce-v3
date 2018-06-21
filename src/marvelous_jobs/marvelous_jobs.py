#!/usr/bin/env python

import argparse

import marvelous_jobs as mj

def parse_args():
    parser = argparse.ArgumentParser(description='Job manager for MARVEL')

    args = parser.parse_args()

    return args

def main():
    args = parse_args()

if __name__ == '__main__':
    main()
