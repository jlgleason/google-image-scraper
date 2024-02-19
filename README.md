This repository contains a python script that scrapes image results from Google/Bing for all queries in a text file. It loads image result pages with a 1920x1080 viewport, collects `n_images` individual image files (and their ranks), and saves a screenshot of the full results page. All image data is saved using base64 encoding to json flat files. The scraper is built on top of `aiohttp`, `pyppeteer`, and `beautifulsoup4` with `lxml`.

### Installation
`pip3 install -r requirements.txt`

### Example Usage

Google: `python3 image_crawler.py -fp qrys.txt -s google -n 50`
Bing: `python3 image_crawler.py -fp qrys.txt -s bing -n 50`

### References 
https://github.com/YunheFeng/CIRF was a helpful reference.