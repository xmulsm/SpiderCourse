# -*- coding: utf-8 -*-
import requests
# requests api：https://requests.kennethreitz.org/en/master/
from lxml import etree
from pandas import DataFrame
import time
import retrying


# 利用retrying模块超时重传
@retrying.retry(stop_max_attempt_number=3)
def get_page(url, headers):
    req = requests.get(url=url, headers=headers, timeout=2)
    return req


def get_fangUrl(baseUrl, pageRange, headers):
    '''
    获取页面所有房源的链接
    :param baseUrl: 基础链接地址
    :param pageRange: 爬取页面范围
    :param headers: 头部信息，安居客有反爬机制，需要传入头部信息主要有cookie和user-agent
    :return: 返回页面的房源链接列表
    '''
    fang_url = []
    for i in pageRange:
        page_url = baseUrl + 'pg' + str(i)
        try:
            r = get_page(url=page_url, headers=headers)
            time.sleep(0.5)  # 设置延时，避免被拒绝服务
            html = etree.HTML(r.text)
            fang_url += html.xpath('//*[@id="content"]/div[1]/ul/li/div[1]/div[1]/a/@href')
        except:
            continue  # 超过设置最大超时重传数则进入下一个循环
    return fang_url


def get_detail_info(url_list, headers):
    '''
    获取房源的详细信息
    :param url_list: 房源链接
    :param headers: 传入头部信息
    :return: 返回房源详细信息列表
    '''
    fangyuan_detail_list = []

    for url in url_list:
        fangyuan_detail = {}
        try:
            req = get_page(url=url, headers=headers)
            time.sleep(1)
            html = etree.HTML(req.text)
            basic_info_label = html.xpath('//*[@id="introduction"]/div/div/div[1]/div[2]/ul/li/span/text()')
            basic_info_text = html.xpath('//*[@id="introduction"]/div/div/div[1]/div[2]/ul/li/text()')
            # print(basic_info_text)
            transaction_info_label = html.xpath('//*[@id="introduction"]/div/div/div[2]/div[2]/ul/li/span[1]/text()')
            transaction_info_text = html.xpath('//*[@id="introduction"]/div/div/div[2]/div[2]/ul/li/span[2]/text()')

            # print(transaction_info_label, transaction_info_text)
            for i in range(len(basic_info_label)):
                fangyuan_detail[basic_info_label[i]] = basic_info_text[i].strip()
            for i in range(len(transaction_info_label)):
                fangyuan_detail[transaction_info_label[i]] = transaction_info_text[i].strip()
            fangyuan_detail['编号'] = html.xpath('/html/body/div[5]/div[2]/div[3]/div[4]/span[2]/text()')[0].strip()
            fangyuan_detail['小区名称'] = html.xpath('/html/body/div[5]/div[2]/div[3]/div[1]/a[1]/text()')[0].strip()
            fangyuan_detail['所在区域'] = html.xpath('/html/body/div[5]/div[2]/div[3]/div[2]/span[2]/a[1]/text()')[
                0].strip()
            fangyuan_detail['二级区域'] = html.xpath('/html/body/div[5]/div[2]/div[3]/div[2]/span[2]/a[2]/text()')[
                0].strip()
            fangyuan_detail['单价(元/平米)'] = html.xpath('/html/body/div[5]/div[2]/div[1]/div[1]/div[1]/span/text()')[
                0].strip()
            fangyuan_detail['总价(万元)'] = html.xpath('/html/body/div[5]/div[2]/div[1]/span[1]/text()')[0].strip()
            fangyuan_detail_list.append(fangyuan_detail)
            print('=============>' + str(url) + '<======爬取完毕===========')
        except:
            continue
    return fangyuan_detail_list


def save_to_csv(fangyuan_detail_list):
    '''
    把房源详细信息保存到csv
    :param fangyuan_detail_list: 房源详细信息列表
    :return: 保存成功与否信息
    '''
    # for detail in fangyuan_detail_list:
    df = DataFrame(fangyuan_detail_list)
    df.to_csv('lianjia_qy.csv', encoding='utf_8_sig', index=False)
    print('保存成功！')


if __name__ == '__main__':
    baseUrl = 'https://qy.lianjia.com/ershoufang/'
    headers = {
        'Host': 'qy.lianjia.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36',
        'Cookie': 'TY_SESSION_ID=22619255-31fd-43c6-b7d6-b5acde1ca0cd; select_city=441800; lianjia_uuid=6f95efcd-6593-4f91-8b70-db14b950b758; _smt_uid=5dbf9371.3cd14812; UM_distinctid=16e3457f5b3114-06e98bcb500304-b363e65-12ae3a-16e3457f5b4ced; sajssdk_2015_cross_new_user=1; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2216e3457f72411a-03c37642953655-b363e65-1224250-16e3457f72543c%22%2C%22%24device_id%22%3A%2216e3457f72411a-03c37642953655-b363e65-1224250-16e3457f72543c%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_referrer%22%3A%22%22%2C%22%24latest_referrer_host%22%3A%22%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%7D%7D; _jzqckmp=1; _ga=GA1.2.879218241.1572836213; _gid=GA1.2.1499680703.1572836213; _jzqx=1.1572849458.1572862600.3.jzqsr=qy%2Elianjia%2Ecom|jzqct=/ershoufang/pg1/.jzqsr=qy%2Elianjia%2Ecom|jzqct=/ershoufang/pg1/; all-lj=8e5e63e6fe0f3d027511a4242126e9cc; lianjia_ssid=9db38b9d-18aa-422a-b7dc-95b04d215378; Hm_lvt_9152f8221cb6243a53c83b956842be8a=1572836210,1572872795; Hm_lpvt_9152f8221cb6243a53c83b956842be8a=1572872795; CNZZDATA1254525948=241562-1572832594-%7C1572869499; CNZZDATA1255633284=898679165-1572834026-%7C1572869133; CNZZDATA1255604082=269358597-1572832096-%7C1572868393; srcid=eyJ0Ijoie1wiZGF0YVwiOlwiMjQ3MTg5YzM3ODc4OTdjMzA5OGMwYzUyZTBkZDk4YjNjNzZhOTAyZGY0ODUwZWRmMDgzY2VlZTRjYmNkMzJiMDM3YTg1NWZjZWIyZmIzOTY3YjMyM2RkMmY1MmQyNzAxZWY4YzgxZmJhZjQxYjg0ODM2YzkxZjlhZDdkODQyNmVkYzVkZmI5NzQyZmJmNjYxMjBjMDVjMmE3YjI1NTdhYjc4ODcwNGM4OGMwNGYzNTE5YzhhNDVkNjE2MmYzZDk0YmE3YjBlZGNjNzFjMDI5OGQ0ZjNiZTA0NzI3MjBjZmU0YTk1OWI1MzYxMGU0YzUwMjFlOTUxYzk4OWJmYWVjOTQ2ZWI4NWVhM2RjMThhODliNDk0M2U0MGEzNzQ2ZmY1MWViNzgwZTQyNjUwMzFhNzFkZjdiNDFlOWJmOWNjYzkwNDRkODZiYjNkZjJkOGU1MjgxOGRkYjIzY2MyMWM5YVwiLFwia2V5X2lkXCI6XCIxXCIsXCJzaWduXCI6XCJkYjBjMDYyOFwifSIsInIiOiJodHRwczovL3F5LmxpYW5qaWEuY29tL2Vyc2hvdWZhbmcvIiwib3MiOiJ3ZWIiLCJ2IjoiMC4xIn0=; _qzja=1.1478090906.1572836210783.1572862600485.1572872796455.1572862600485.1572872796455.0.0.0.26.6; _qzjc=1; _qzjto=26.6.0; _jzqa=1.2904530830101713400.1572836211.1572862600.1572872796.6; _jzqc=1; _qzjb=1.1572872796454.1.0.0.0; _jzqb=1.1.10.1572872796.1; _gat_global=1; _gat_new_global=1; _gat_dianpu_agent=1',
    }

    url_all = get_fangUrl(baseUrl, range(1, 101), headers)
    print(url_all)
    fangyuan = get_detail_info(url_all, headers)
    print(fangyuan)
    save_to_csv(fangyuan)
