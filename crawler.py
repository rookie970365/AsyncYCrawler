import asyncio
import logging
import os
from collections import namedtuple
from random import randint

import aiofiles
import aiohttp
import async_timeout
from bs4 import BeautifulSoup

BASE_URL = "https://news.ycombinator.com/"
TIME_BETWEEN_REQUESTS = 600
FETCH_TIMEOUT = 10
OUTPUT_DIR = "./results/"

Item = namedtuple("Item", "id, ref, comments_ref")
downloaded_ids = []


def parse_main_page(html):
    """функция собирает новостные ссылки с основной страницы"""
    soup = BeautifulSoup(html, "lxml")
    tr_list = soup.find_all("tr", class_="athing")
    item_news_list = []
    for tr in tr_list:
        ref = tr.find("span", class_="titleline").find("a").get("href")
        _id = tr.get("id")
        if _id in downloaded_ids:
            continue
        comments_ref = f"{BASE_URL}/item?id={_id}"
        if not ref.startswith("http"):
            ref = f"{BASE_URL}/{ref}"
        item_news_list.append(Item(_id, ref, comments_ref))
        downloaded_ids.append(_id)
    return item_news_list


def parse_comment_page(html):
    """функция собирает ссылки со html-страницы комментариев"""
    soup = BeautifulSoup(html, "lxml")
    span_tags = soup.find_all("span", class_="commtext c00")
    ref_list = []
    for span in span_tags:
        for a in span.find_all("a"):
            ref_list.append(a.get("href"))
    return ref_list


async def fetch(session, url):
    """функция извлечения данных из ответа сервера"""
    async with session.get(url, verify_ssl=False) as response:
        async with async_timeout.timeout(FETCH_TIMEOUT):
            return await response.text()


async def save_file(html, path):
    """функция записи данных в файл"""
    async with aiofiles.open(path, "wb") as f:
        await f.write(html.encode("utf-8"))


async def process_news(session, item):
    """функция обработки единицы новостей"""
    logging.info("Start processing item of news with id %s: %s", item.id, item.ref)
    try:
        html = await fetch(session, item.ref)
        await save_file(html, f"{OUTPUT_DIR}news_{item.id}.html")
    except Exception as e:
        logging.error("Error retrieving from %s: %s", item.id, e)


async def process_comments(session, item):
    """функция обработки страницы комментариев к новости"""
    comment_page_html = await fetch(session, item.comments_ref)
    comments_ref_list = parse_comment_page(comment_page_html)
    if comments_ref_list:
        logging.info(
            "Start processing of %s links from comments page %s",
            len(comments_ref_list),
            item.id,
        )
        item_dir = os.path.join(OUTPUT_DIR, item.id)
        os.makedirs(item_dir, exist_ok=True)
        for ref in comments_ref_list:
            try:
                html = await fetch(session, ref)
                await save_file(html, f"{OUTPUT_DIR}{item.id}/{randint(10, 99)}.html")
            except Exception as e:
                logging.error("Error retrieving from: %s, %s", ref, e)


async def main():
    """основная функция"""
    while True:
        async with aiohttp.ClientSession() as session:
            main_page_html = await fetch(session, BASE_URL)
            new_item_list = parse_main_page(main_page_html)
            logging.info("Crawler found %s new links", len(new_item_list))
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            await asyncio.gather(*[process_news(session, item) for item in new_item_list])
            await asyncio.gather(*[process_comments(session, item) for item in new_item_list])
            logging.info('Crawler end processing')

            await asyncio.sleep(TIME_BETWEEN_REQUESTS)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname).1s %(message)s",
        datefmt="%Y.%m.%d %H:%M:%S",
    )
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt. Stopping...")
