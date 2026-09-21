import os, sys, json, zlib, hashlib, time

HERE = os.path.dirname(os.path.abspath(__file__))
CHUNKS_DIR = os.path.join(HERE, 'data-paper-v61', 'full_db_chunks')
MANIFEST_PATH = os.path.join(CHUNKS_DIR, 'manifest.json')
OUTPUT_PATH = os.path.join(HERE, 'data-paper-v61', 'radar.db')

def main():
    if not os.path.exists(MANIFEST_PATH):
        print('Error: Manifest not found at', MANIFEST_PATH)
        sys.exit(1)
    with open(MANIFEST_PATH, 'r') as f:
        manifest = json.load(f)
    expected_sha = manifest['original_sha256']
    expected_size = manifest['original_size']
    total_parts = manifest['total_parts']
    print('================================================================')
    print('  Compression Radar 6.1 — Full Database Reassembly')
    print('  Target:', OUTPUT_PATH)
    print(f'  Expected size: {expected_size / (1024*1024*1024):.2f} GB ({expected_size:,} bytes)')
    print('  Total parts:', total_parts)
    print('================================================================')
    if os.path.exists(OUTPUT_PATH) and os.path.getsize(OUTPUT_PATH) == expected_size:
        print('Checking existing radar.db hash...')
        sha = hashlib.sha256()
        with open(OUTPUT_PATH, 'rb') as f:
            while True:
                chunk = f.read(16 * 1024 * 1024)
                if not chunk: break
                sha.update(chunk)
        if sha.hexdigest() == expected_sha:
            print('[OK] radar.db is already present and fully verified!')
            return
    t0 = time.time()
    decompressor = zlib.decompressobj()
    sha = hashlib.sha256()
    bytes_written = 0
    tmp_out = OUTPUT_PATH + '.tmp'
    with open(tmp_out, 'wb') as out:
        for part_num in range(1, total_parts + 1):
            part_filename = f'part_{part_num:03d}.bin'
            part_path = os.path.join(CHUNKS_DIR, part_filename)
            if not os.path.exists(part_path):
                print('Error: Missing', part_path)
                sys.exit(1)
            part_size_mb = os.path.getsize(part_path) / (1024 * 1024)
            print(f'  Decompressing {part_filename} ({part_size_mb:.1f} MB) [{part_num}/{total_parts}]...', end='', flush=True)
            with open(part_path, 'rb') as p_in:
                while True:
                    compressed_chunk = p_in.read(8 * 1024 * 1024)
                    if not compressed_chunk: break
                    raw = decompressor.decompress(compressed_chunk)
                    if raw:
                        out.write(raw)
                        sha.update(raw)
                        bytes_written += len(raw)
            print(f' -> Output: {bytes_written / (1024*1024):.1f} MB')
        tail = decompressor.flush()
        if tail:
            out.write(tail)
            sha.update(tail)
            bytes_written += len(tail)
    actual_sha = sha.hexdigest()
    if actual_sha != expected_sha:
        print(f'[ERROR] Hash mismatch! Expected: {expected_sha}, Actual: {actual_sha}')
        if os.path.exists(tmp_out): os.remove(tmp_out)
        sys.exit(1)
    if os.path.exists(OUTPUT_PATH): os.remove(OUTPUT_PATH)
    os.rename(tmp_out, OUTPUT_PATH)
    dt = time.time() - t0
    print('================================================================')
    print(f'  [SUCCESS] Verified full database restored in {dt:.1f}s!')
    print(f'  File: {OUTPUT_PATH}')
    print(f'  Size: {bytes_written:,} bytes ({bytes_written / (1024*1024*1024):.2f} GB)')
    print(f'  SHA-256: {actual_sha} (MATCHED)')
    print('================================================================')

if __name__ == '__main__':
    main()
