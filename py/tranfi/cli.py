"""
cli.py — Tranfi CLI entry point.

Usage:
  tranfi 'csv | filter "age > 25" | csv'  < in.csv
  tranfi -f pipeline.tf < in.csv > out.csv
  tranfi -i input.csv -o output.csv 'csv | sort age | csv'
  tranfi profile < data.csv
"""

import argparse
import sys
import json
from . import _ffi

CHUNK_SIZE = 64 * 1024
CHAN_MAIN = 0
CHAN_ERRORS = 1
CHAN_STATS = 2


def format_bytes(n):
    if n < 1024:
        return f'{n}B'
    elif n < 1024 * 1024:
        return f'{n / 1024:.1f}KB'
    elif n < 1024 * 1024 * 1024:
        return f'{n / (1024 * 1024):.1f}MB'
    else:
        return f'{n / (1024 * 1024 * 1024):.1f}GB'


def main():
    parser = argparse.ArgumentParser(
        prog='tranfi',
        description='Streaming ETL with a pipe-style DSL.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''\
examples:
  tranfi 'csv | csv'                           # passthrough
  tranfi 'csv | filter "age > 25" | csv'       # filter rows
  tranfi 'csv | select name,age | csv'         # select columns
  tranfi 'csv | head 10 | csv'                 # first N rows
  tranfi 'csv | sort age | csv'                # sort by column
  tranfi 'csv | stats | jsonl'                 # aggregate stats
  tranfi profile                               # built-in recipe
''')
    parser.add_argument('pipeline', nargs='?', help='DSL string or recipe name')
    parser.add_argument('-f', '--file', help='read pipeline from file')
    parser.add_argument('-i', '--input', help='read input from file instead of stdin')
    parser.add_argument('-o', '--output', help='write output to file instead of stdout')
    parser.add_argument('-j', '--json', action='store_true', help='compile only, output JSON plan')
    parser.add_argument('-q', '--quiet', action='store_true', help='suppress stats on stderr')
    parser.add_argument('-p', '--progress', action='store_true', help='show progress on stderr')
    parser.add_argument('-v', '--version', action='store_true', help='show version')
    parser.add_argument('-R', '--recipes', action='store_true', help='list built-in recipes')

    args = parser.parse_args()

    if args.version:
        print(f'tranfi {_ffi.version()}')
        return

    if args.recipes:
        n = _ffi.recipe_count()
        print(f'Built-in recipes ({n}):\n')
        for i in range(n):
            print(f'  {_ffi.recipe_name(i):<12s} {_ffi.recipe_description(i)}')
            print(f'  {"":12s} {_ffi.recipe_dsl(i)}')
            print()
        return

    # Get pipeline text
    if args.file:
        with open(args.file, 'r') as f:
            pipeline_text = f.read()
    elif args.pipeline:
        pipeline_text = args.pipeline
    else:
        parser.error('no pipeline specified')

    # Parse: recipe name, JSON, or DSL
    pt = pipeline_text.strip()
    if pt.startswith('{'):
        plan_json = pt
    elif '|' not in pt and ' ' not in pt:
        # Try built-in recipe
        recipe_dsl = _ffi.recipe_find_dsl(pt)
        if recipe_dsl:
            plan_json = _ffi.compile_dsl(recipe_dsl)
        else:
            plan_json = _ffi.compile_dsl(pipeline_text)
    else:
        plan_json = _ffi.compile_dsl(pipeline_text)

    # JSON mode: print and exit
    if args.json:
        print(plan_json)
        return

    # Create pipeline
    handle = _ffi.pipeline_create_from_json(plan_json)

    try:
        # Open I/O
        if args.input:
            fin = open(args.input, 'rb')
        else:
            fin = sys.stdin.buffer

        if args.output:
            fout = open(args.output, 'wb')
        else:
            fout = sys.stdout.buffer

        # Stream input
        total_bytes = 0
        while True:
            chunk = fin.read(CHUNK_SIZE)
            if not chunk:
                break
            _ffi.pipeline_push(handle, chunk)
            total_bytes += len(chunk)

            # Pull output immediately (streaming)
            while True:
                data = _ffi.pipeline_pull(handle, CHAN_MAIN)
                if not data:
                    break
                fout.write(data)

            if args.progress:
                sys.stderr.write(f'\r{format_bytes(total_bytes)} processed')

        # Finish
        _ffi.pipeline_finish(handle)

        # Pull remaining output
        while True:
            data = _ffi.pipeline_pull(handle, CHAN_MAIN)
            if not data:
                break
            fout.write(data)
        fout.flush()

        if args.progress:
            sys.stderr.write(f'\r{format_bytes(total_bytes)} processed (done)\n')

        # Errors to stderr
        while True:
            data = _ffi.pipeline_pull(handle, CHAN_ERRORS)
            if not data:
                break
            sys.stderr.buffer.write(data)

        # Stats to stderr (unless quiet)
        if not args.quiet:
            while True:
                data = _ffi.pipeline_pull(handle, CHAN_STATS)
                if not data:
                    break
                sys.stderr.buffer.write(data)

    finally:
        _ffi.pipeline_free(handle)
        if args.input and fin is not sys.stdin.buffer:
            fin.close()
        if args.output and fout is not sys.stdout.buffer:
            fout.close()


if __name__ == '__main__':
    main()
