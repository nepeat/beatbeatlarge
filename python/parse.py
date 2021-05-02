import os
import glob
import io
import redis
import time
# import orjson

import multiprocessing

import zstandard

def zstd_streamer(filename: str, chunk_size: int=1024 * 1024 * 64):
    with open(filename, 'rb') as fh:
        dctx = zstandard.ZstdDecompressor()
        with dctx.stream_reader(fh) as reader:
            while True:
                chunk = reader.read(chunk_size)
                if not chunk:
                    break
                yield len(chunk)
            # text_stream = io.TextIOWrapper(reader, encoding="utf8")
            # for line in text_stream:
            #     yield line

def count_lines(filename: str):
    i = 0
    last_check = time.time()
    print(f"[{filename}] line count starting")

    for line in zstd_streamer(filename):
        i += line
        # print(line)
        # orjson.loads(line)
        # i += 1
        if True:
            new_time = time.time()
            delta = new_time - last_check
            last_check = new_time
            print(f'[{filename}] {i:,}, {delta:.4}s')
    print(f"[{filename}] {i:,} lines.")

    return i

def main():
    r = redis.Redis()

    # Parsing
    # for file in glob.glob("output/*.txt.zst"):
    #     print(count_lines(file))

    with multiprocessing.Pool(processes=4) as pool:
        total_lines = sum(pool.map(count_lines, glob.glob("output/*.txt.zst")))
        print(f"{total_lines,} in all files.")



if __name__ == "__main__":
    # asyncio.run(main())
    main()