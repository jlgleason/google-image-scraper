import os
import base64
import json
import asyncio
from time import time
import argparse
from urllib.parse import quote_plus

import aiohttp
from pyppeteer import launch
from bs4 import BeautifulSoup


BASE_URLS = {
    "google": "https://www.google.com/search",
    "bing": "https://www.bing.com/images/feed",
}
OPTS = {"google": {}, "bing": {"waitUntil": "networkidle0"}}
SEARCH_BAR = {"google": "input[title='Search']", "bing": "input[type='search']"}
IMG_CLS = {"google": "rg_i Q4LuWd", "bing": "mimg"}
NO_RESULTS = {
    "google": "Looks like there aren’t any matches for your search",
    "bing": "There are no results for",
}
DATA_START = "data:image/"


def log_error(qry, img_rank, error, error_type, fp_log_errors):
    """log error and error type"""
    error_str = "\t".join([qry, str(img_rank), error, error_type])
    with open(fp_log_errors, "a") as f:
        f.write(error_str + "\n")
        f.flush()


def build_url(qry):
    """build url using query and special url parameters"""
    params = {"q": quote_plus(qry)}

    if args.sengine == "google":
        base_url = "https://www.google.com/search"
        params["tbm"] = "isch"
    elif args.sengine == "bing":
        base_url = "https://www.bing.com/images/search"

    param_str = "&".join([f"{k}={v}" for k, v in params.items()])
    return f"{base_url}?{param_str}"


def log_img(qry, img_rank, img_data, fp_output_imgs):
    """log image"""
    with open(fp_output_imgs, "a") as f:
        f.write(
            json.dumps(
                {
                    "qry": qry,
                    "img_b64": img_data,
                    "img_rank": img_rank,
                }
            )
            + "\n"
        )
        f.flush()


def process_base64(img_src, img_rank, qry, fp_output_imgs):
    """log base64 encoded image"""
    log_img(qry, img_rank, img_src.split("base64,")[-1], fp_output_imgs)


async def process_url(session, img_src, img_rank, qry, fp_output_imgs, fp_log_errors):
    """retrieve image from URL"""
    async with session.get(img_src) as r:
        if r.status == 200:
            img_b = await r.read()
            log_img(qry, img_rank, base64.b64encode(img_b).decode(), fp_output_imgs)
        else:
            log_error(qry, img_rank, img_src, "request", fp_log_errors)


async def write_images(imgs, qry, n_images, fp_output_imgs, fp_log_errors):
    """write individual images to logs"""
    # builds from https://github.com/YunheFeng/CIRF/blob/main/1_preprocess_images_Google.py

    if args.sengine == "bing":
        img_prts = [img.find_parent("li") for img in imgs]
        imgs = [img for img, prt in zip(imgs, img_prts) if prt]
        img_idxs = [int(prt["data-idx"]) for prt in img_prts if prt]
        imgs = [img for img, _ in sorted(zip(imgs, img_idxs), key=lambda pair: pair[1])]

    async with aiohttp.ClientSession() as session:

        for img_rank, img in enumerate(imgs):

            if img_rank >= n_images:
                break

            if "src" in img.attrs:
                img_src = img.attrs["src"]
            elif "data-src" in img.attrs:
                img_src = img.attrs["data-src"]
            else:
                log_error(
                    qry, img_rank, ",".join(img.attrs.keys()), "attr", fp_log_errors
                )

            if img_src.startswith(DATA_START):
                process_base64(img_src, img_rank, qry, fp_output_imgs)
            elif img_src.startswith("https://"):
                await process_url(
                    session, img_src, img_rank, qry, fp_output_imgs, fp_log_errors
                )
            else:
                log_error(qry, img_rank, img_src[:10], "start", fp_log_errors)


async def load_url(page, qry):
    """load search URL"""

    try:
        if args.sengine == "google":
            url = build_url(qry)
            await page.goto(url)
        elif args.sengine == "bing":
            await page.goto(BASE_URLS[args.sengine], options=OPTS[args.sengine])
            await page.waitForSelector(SEARCH_BAR[args.sengine])
            await page.type(SEARCH_BAR[args.sengine], qry)
            navPromise = asyncio.ensure_future(
                page.waitForNavigation(options=OPTS[args.sengine])
            )
            await page.keyboard.press("Enter")
            await navPromise
        return True
    except Exception as e:
        print(e)
        print(qry)
        await page.close()
        return False


async def parse_images(
    page,
    qry,
    n_images,
    fp_output_imgs,
    fp_output_screens,
    dir_logs,
    fp_log_success,
    fp_log_errors,
):
    """parse images from html and write to log"""

    html = await page.content()
    soup = BeautifulSoup(html, "lxml")
    imgs = soup.find_all("img", class_=IMG_CLS[args.sengine])

    if len(imgs):
        await write_images(imgs, qry, n_images, fp_output_imgs, fp_log_errors)
        full_b64 = await page.screenshot({"encoding": "base64"})
        with open(fp_output_screens, "a") as f:
            f.write(json.dumps({"qry": qry, "img_b64": full_b64}) + "\n")
            f.flush()
        with open(fp_log_success, "a") as f:
            f.write(qry + "\n")
            f.flush()

        print(f"parsed qry {qry}: {len(imgs)} imgs")
        await page.close()
        return True

    # legit no results
    elif NO_RESULTS[args.sengine] in soup.get_text("|", strip=True):
        with open(fp_log_success, "a") as f:
            f.write(qry + "\n")
            f.flush()
        print(f"no results for {qry}")
        await page.close()
        return True

    else:
        print(f"0 images found for {qry} -- suggests BLOCKED")
        await page.screenshot(
            {"path": os.path.join(dir_logs, f"{args.sengine}_blocked.png")}
        )
        await page.close()
        return False


async def crawl_qry(
    browser,
    qry,
    n_images,
    fp_output_imgs,
    fp_output_screens,
    dir_logs,
    fp_log_success,
    fp_log_errors,
):
    """crawl a single query"""
    page = await browser.newPage()
    await page.setViewport({"width": 1920, "height": 1080})
    success = await load_url(page, qry)
    if success:
        return await parse_images(
            page,
            qry,
            n_images,
            fp_output_imgs,
            fp_output_screens,
            dir_logs,
            fp_log_success,
            fp_log_errors,
        )
    else:
        return False


def update_todo(qrys, fp_log_success):
    """update uncrawled queries"""
    if os.path.exists(fp_log_success):
        with open(fp_log_success, "r") as f:
            crawled_qrys = [l.strip() for l in f.readlines()]
        return list(set(qrys).difference(set(crawled_qrys)))
    else:
        return qrys


async def main(
    qrys,
    n_images,
    n_parallel,
    fp_output_imgs,
    fp_output_screens,
    dir_logs,
    fp_log_success,
    fp_log_errors,
):
    """a) launch browswer, b) fan out requests and crawl"""

    if args.test:
        browser = await launch()
        await crawl_qry(
            browser,
            qrys[0],
            n_images,
            fp_output_imgs,
            fp_output_screens,
            dir_logs,
            fp_log_success,
            fp_log_errors,
        )
        await browser.close()

    else:
        try:
            success_rate = 1.0
            while success_rate > args.success_rate and len(qrys):
                st = time()
                qrys = update_todo(qrys, fp_log_success)
                print(f"{len(qrys)} queries to crawl")
                if len(qrys):
                    browser = await launch()
                    tasks = [
                        asyncio.ensure_future(
                            crawl_qry(
                                browser,
                                qry,
                                n_images,
                                fp_output_imgs,
                                fp_output_screens,
                                dir_logs,
                                fp_log_success,
                                fp_log_errors,
                            )
                        )
                        for qry in qrys[:n_parallel]
                    ]
                    success = await asyncio.gather(*tasks)
                    success_rate = sum(success) / len(success)
                    print(f"crawling {len(success)} queries took {time()-st}s")
                    print(f"success rate: {success_rate}")
                    await browser.close()

            if success_rate <= args.success_rate:
                print(f"success rate fell below {args.success_rate}, killing process")
            else:
                print("crawled all queries")

        finally:
            await browser.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Scrape Google/Bing Image search results"
    )
    parser.add_argument(
        "-fp",
        "--fp_qrys",
        default="qrys.txt",
        type=str,
        help="Text file with one query on each line. Default='fp_qrys'",
    )
    parser.add_argument(
        "-s",
        "--sengine",
        default="google",
        type=str,
        help="Which search engine to crawl. Default='google'",
    )
    parser.add_argument(
        "-n",
        "--n_images",
        default=50,
        type=int,
        help="Number of images to collect from each results page. Default=50",
    )
    parser.add_argument(
        "-p",
        "--n_parallel",
        default=10,
        type=int,
        help="Number of queries to crawl in parallel. Default=25",
    )
    parser.add_argument(
        "-r",
        "--success_rate",
        default=0.5,
        type=float,
        help="Kill crawl if success rate falls below this number. Default=0.5",
    )
    parser.add_argument(
        "-d",
        "--dir_output",
        default="data",
        type=str,
        help="Output directory for data. Default='data'",
    )
    parser.add_argument(
        "-l",
        "--dir_logs",
        default="logs",
        type=str,
        help="Output directory for logs. Default='logs'",
    )
    parser.add_argument(
        "--test",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="Whether to run in test mode (scrape 1 query). Default=False",
    )
    args = parser.parse_args()

    with open(args.fp_qrys) as f:
        qrys = [l.strip() for l in f.readlines()]

    os.makedirs(args.dir_output, exist_ok=True)
    os.makedirs(args.dir_logs, exist_ok=True)

    fp_output_imgs = os.path.join(args.dir_output, f"{args.sengine}_imgs.json")
    fp_output_screens = os.path.join(
        args.dir_output, f"{args.sengine}_screenshots.json"
    )
    fp_log_success = os.path.join(args.dir_logs, f"{args.sengine}_success.txt")
    fp_log_errors = os.path.join(args.dir_logs, f"{args.sengine}_errors.tsv")

    asyncio.run(
        main(
            qrys,
            args.n_images,
            args.n_parallel,
            fp_output_imgs,
            fp_output_screens,
            args.dir_logs,
            fp_log_success,
            fp_log_errors,
        )
    )
