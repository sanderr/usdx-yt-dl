# usdx-yt-dl

This is a quick-and-dirty script to automate the download of required files, given a txt file containing a
[USDX](https://github.com/UltraStar-Deluxe/USDX) song description. It currently only works for files that contain
a structured reference to the associated video's id, as I noticed many txts on https://usdb.animux.de do.

At the moment, it is still a rough implementation, fairly untested, very tailored to my use case and badly documented.
I'll probably clean it up at some point and make it more generic. For now, I'd recommend using it only after inspecting
the source to make sure you know what to expect.

The script has been developed for Python 3.10 but it may or may not work with older Python version. I took care to only use
built-in libraries, so no packages need to be installed.
