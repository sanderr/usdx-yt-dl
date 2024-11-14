# usdx-yt-dl

This is a quick-and-dirty script to automate the download of required files, given a txt file containing a
[USDX](https://github.com/UltraStar-Deluxe/USDX) song description. It currently only works for files that contain
a structured reference to the associated video's id, as I noticed many txts on https://usdb.animux.de do.

At the moment, it is still a rough implementation, fairly untested, very tailored to my use case and badly documented.
I'll probably clean it up at some point and make it more generic. For now, I'd recommend using it only after inspecting
the source to make sure you know what to expect.

The script has been developed for Python 3.10 but it may or may not work with older Python version. I took care to only use
built-in libraries, so no packages need to be installed.


# How to run

1. Install uv either through your distro's package manager (recommended), or in a venv (instructions below)
```sh
python -m venv .env
source .env/bin/activate
pip install -U pip uv
```

2. Optionally install [rsgain](https://github.com/complexlogic/rsgain) if you want your downloaded mp3s
    to be replaygain tagged. USDX does not support this yet, but there is a
    [feature request](https://github.com/UltraStar-Deluxe/USDX/issues/638).

3. execute the script: `./usdx-yt-dl.py <path-to-bulk-songs-dir>` or explicitly with uv:
    `uv run usdx-yt-dl.py <path-to-bulk-songs-dir>`. Uv will make sure to install all
    required dependencies before running it.
