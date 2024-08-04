Araneid
=======

.. image:: https://https://github.com/WALL-EEEEEEE/araneid/badges/master/pipeline.svg
   :target: https://https://github.com/WALL-EEEEEEE/araneid/-/pipelines
   :alt: pipeline status
    
.. image:: https://https://github.com/WALL-EEEEEEE/araneid/badges/test/coverage.svg
   :target: https://github.com/WALL-EEEEEEE/araneid/coverage/test
   :alt: coverage report

Doc: **English** **Chinese**

=================
Introduction
=================

``Aranied`` is designed to be a  highly intergrated, configurable, and out of box crawler framework for data.


=================
Requiement
=================
* ``>=python3.7``

=================
Install
=================

.. code-block:: bash
    pip3 install git+https://github.com/WALL-EEEEEEE/araneid

=================
Simple  example
=================

.. code-block:: python
    # save as GoogleSpider.py
    from araneid.crawlers.default import DefaultCrawler  # 引入默认的爬虫类
    from araneid.network.http import HttpRequest, HttpResponse
    from araneid.annotation import CrawlerCliRunner


    @CrawlerCliRunner  # Make this script is runnable under cli
    class GoogleSpider(DefaultCrawler):  # Class name corresponding with file name is recommended.
        name = 'GoogleSpider'  # Define the spider name

        # Initialize start requests 
        def start_requests(self):
            req = HttpRequest(url='http://www.google.com', callbacks=[self.parse])  # generate the request and bind response to a specific parser( `GoogleSpider.parse` here). 
            yield req  # yield the request to the engine

        # Define a parser
        def parse(self, response:HttpResponse):
            status = response.status_code  # Get status code of your http request
            url = response.request.uri  # Get url of your http request
            content = response.text # Access your http response content
            if status == 200:  # If it succeeded?
                print('Get content from ' + url + ":<" + content + '>')

> The sample code can be checkout at：[demo/GoogleSpider.py](https://github.com/WALL-EEEEEEE/araneid/blob/master/demo/GoogleSpider.py)

.. code-block:: bash
    # Run
    araneid run GoogleSpider.py
    # Expected output as bellow
    Get content from http://www.google.com:<<!doctype html><html itemscope="" itemtype="http://schema.org/WebPage" lang="zh-TW"><head><meta content="text/html; charset=UTF-8" http-equiv="Content-Type"><meta content="/images/branding/googleg/1x/googleg_standard_color_128dp.png" itemprop="image"><title>Google</title>...</html>

=================
Details
=================

Detail usages please refer to: [Document](https://github.com/WALL-EEEEEEE/araneid/blob/master/doc/zh_cn/Start.md)


