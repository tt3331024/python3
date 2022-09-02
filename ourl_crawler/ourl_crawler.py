# coding: utf-8
""" ourl crawler 
爬取目標網址的meta title, description, image, site_name.
部屬上Cloud Functions

Created on 2021/09/28 by Jerry.Ko
"""
import asyncio
import requests
from bs4 import BeautifulSoup, element
from flask import json
from pyppeteer import launch
from re import match
from urllib.parse import urljoin

def isNoneOrWhiteSpace(str=None):
    """判斷是否為 None or 空值 or 空白
    Examples
    --------
    >>> tests = ['foo', ' ', '\r\n\t', '', None]
    >>> [not s or s.isspace() for s in tests]
    [False, True, True, True, True]
    Args:
      str: str. 帶判斷的物件
    """
    return not str or str.isspace()


def parse_title(soup):
    """取得title。順序為 og:title > head title > 'none'
    Args:
      soup: bs4.BeautifulSoup. bs4解析後的網頁內容。
    return:
      output: str. title 內容
    """
    output = None
    if isNoneOrWhiteSpace(output):
        try:
            css_selector = soup.select_one("meta[property='og:title']")['content']
            output = css_selector
        except Exception as e:
            pass
    
    if isNoneOrWhiteSpace(output):
        try:
            css_selector = soup.select_one("head title").text.strip()
            output = css_selector
        except Exception as e:
            pass
    
    if isNoneOrWhiteSpace(output):
        return "none"
    
    return output


def parse_description(soup):
    """取得description。順序為 og:description > description > h1 > h2 > h3 > parse_title
    Args:
      soup: bs4.BeautifulSoup. bs4解析後的網頁內容。
    return:
      output: str. description 內容
    """
    output = None
    if isNoneOrWhiteSpace(output):
        try:
            css_selector = soup.select_one("meta[property='og:description']")['content']
            output = css_selector
        except Exception as e:
            pass
    if isNoneOrWhiteSpace(output):
        try:
            css_selector = soup.select_one("meta[name='description' i]")['content']
            output = css_selector
        except Exception as e:
            pass
    if isNoneOrWhiteSpace(output):
        try:
            css_selector = soup.select_one("h1").text.strip()
            output = css_selector
        except Exception as e:
            pass
    if isNoneOrWhiteSpace(output):
        try:
            css_selector = soup.select_one("h2").text.strip()
            output = css_selector
        except Exception as e:
            pass
    if isNoneOrWhiteSpace(output):
        try:
            css_selector = soup.select_one("h3").text.strip()
            output = css_selector
        except Exception as e:
            pass
    if isNoneOrWhiteSpace(output):
        return parse_title(soup)
    return output


def parse_image(soup, url):
    """取得image。順序為 og:image > link[rel|='apple-touch-icon'] > link[rel*='icon'] > img > 'none'
    如果image為相對路徑，則加上domain。
    Args:
      soup: bs4.BeautifulSoup. bs4解析後的網頁內容。
    return:
      output: str. title 內容
    """
    domain = match("^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/?\n]+)", url)[0]
    output = None
    if isNoneOrWhiteSpace(output):
        try:
            css_selector = soup.select_one("meta[property='og:image']")['content']
            output = css_selector
        except Exception as e:
            pass
    if isNoneOrWhiteSpace(output):
        try:
            css_selector = soup.select_one("link[rel|='apple-touch-icon']")['content']
            output = css_selector
        except Exception as e:
            pass   
    if isNoneOrWhiteSpace(output):
        try:
            css_selector = soup.select_one("link[rel*='icon']")['content']
            output = css_selector
        except Exception as e:
            pass
    if isNoneOrWhiteSpace(output):
        try:
            css_selector = soup.select_one("img")['src']
            output = css_selector
        except Exception as e:
            pass
    if isNoneOrWhiteSpace(output):
        return "none"
    if "://" not in output:
        output = urljoin(domain, output)
    return output


def parse_name(soup):
    """取得 site_name。順序為 og:site_name > parse_title
    Args:
      soup: bs4.BeautifulSoup. bs4解析後的網頁內容。
    return:
      output: str. title 內容
    """
    output = None
    if isNoneOrWhiteSpace(output):
        try:
            css_selector = soup.select_one("meta[property='og:site_name']")['content']
            output = css_selector
        except Exception as e:
            pass
    if isNoneOrWhiteSpace(output):
        return parse_title(soup)
    return output

def requests_crawler(url):
    print("used requests method")
    req = requests.get(url) 
    # 處理編碼
    req.encoding = req.apparent_encoding
    soup = BeautifulSoup(req.text, 'lxml')
    # return soup
    title_test = soup.select_one("head title")
    print("req.status_code: {}\ntitle_test: {}".format(req.status_code, title_test))
    if req.status_code != 200 or title_test is None:
        return "spa"
    else:
        # return soup
        t = parse_title(soup)
        d = parse_description(soup)
        n = parse_name(soup)
        i = parse_image(soup, url)
        url_info_dict = {"title": t, "desc": d, "name": n, "image": i}
        return url_info_dict

async def pyppeteer_crawler(url):
    print("used pyppeteer method")
    # 開啟瀏覽器程序
    browser = await launch(
        handleSIGINT=False,
        handleSIGTERM=False,
        handleSIGHUP=False
    )
    # 用瀏覽器連到 url
    page = await browser.newPage()
    await page.goto(url)    
    await asyncio.sleep(2)
    
    html_doc = await page.content()
    soup = BeautifulSoup(html_doc, 'lxml')
    await browser.close()
#     return soup  
    t = parse_title(soup)
    d = parse_description(soup)
    n = parse_name(soup)
    i = parse_image(soup, url)
    url_info_dict = {"title": t, "desc": d, "name": n, "image": i}
    return url_info_dict

def main(param):
    # url = param
    request_json = param.get_json(silent=True)
    url = request_json['url']
    print("url: {}".format(url))
    # use requests crawler 
    output = requests_crawler(url)
    # 如果為SPA網頁，抓不到 title，則使用pyppeteer
    if output == 'spa':
        output = asyncio.run(pyppeteer_crawler(url))
    return json.dumps(output, ensure_ascii=False, indent=2)