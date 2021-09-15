import re
import os
import logging

import requests
from bs4 import BeautifulSoup

from helpers import timer

if not os.path.exists("logs"):
    os.mkdir("logs")

logging.basicConfig(
    filename="logs/app.log",
    level=logging.INFO,
    format="%(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("scraper")


@timer
def scrape():
    """Scrape Amazon latest books page"""

    amazon_url = "https://www.amazon.fr/gp/new-releases/books/ref=zg_bsnr_nav_0"
    page = requests.get(amazon_url)
    soup = BeautifulSoup(page.content, "html.parser")
    logger.info("Got page content. Ready to scrape..")

    books = []

    try:
        results = soup.find_all("span", class_="aok-inline-block zg-item")
        for item in results:
            book = {}

            # title
            title_link = item.find("a", class_="a-link-normal")

            title = title_link.find("div", class_="p13n-sc-truncate").text.strip()
            book["title"] = title

            # author
            author = item.find("div", class_="a-row a-size-small").text
            book["author"] = author

            # format
            format = item.find_all("div", class_="a-row a-size-small")[1].text
            book["format"] = format

            # price
            price_div = item.find_all("a", class_="a-link-normal")
            if len(price_div) > 1:
                price = price_div[-1].find("span", class_="p13n-sc-price")
                if price:
                    price = price.text
                    book["price"] = price
                else:
                    price_div2 = item.find_all("div", class_="a-row")[-1]
                    price_2 = price_div2.find("a", class_="a-link-normal a-text-normal")
                    if price_2:
                        price_2 = price_2.find(
                            "span", class_="a-size-base a-color-price"
                        )
                        price = price_2.find("span", class_="p13n-sc-price").text
                        book["price"] = price
            else:
                pass  # No price

            # Rating
            rating = item.find("span", class_="a-icon-alt")
            if rating:
                rating = rating.text
                book["rating"] = rating

            # Number of comments
            comments = item.find_all("a", href=re.compile("/product-reviews/"))
            if len(comments) > 1:
                comments = comments[1].text
                book["comments"] = comments

            # image
            img_div = title_link.find("div", class_="a-section a-spacing-small")
            img_url = img_div.find("img")["src"]
            book["image"] = img_url

            books.append(book)

        return books
    except:
        logger.error("Error in scraping")
