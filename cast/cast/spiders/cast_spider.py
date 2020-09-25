from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from datetime import datetime
import time

class moviespider(CrawlSpider):
    name = "cast"
    unique_id = 0
    # seed url
    # type of comedy is limited to movie and show
    # genres is limited to comedy
    start_urls = ['https://www.imdb.com/search/name/?gender=male,female&count=100',]
	# limit the number of movie we retrive
    custom_settings = {'CLOSESPIDER_ITEMCOUNT': 5400}

    # next page
    rules = [Rule(LinkExtractor(restrict_css=('div.desc a')),follow=True,callback='parse_listpage'),]

    def parse_listpage(self,response):
    	# page that show the list of comedy movie

    	# links for each comedy movie
    	queues = response.xpath('//div[@class="lister-item-content"][contains(.//p[@class="text-muted text-small"],"Actor")'
    		' or contains(.//p[@class="text-muted text-small"],"Actress")]/h3/a/@href').getall()
    	for i in queues:
    		i += "/bio?ref_=nm_ov_bio_sm"

    		yield response.follow(i,callback=self.parse_detail)

    def parse_detail(self, response):
    	# output detail
        data = {}
        self.unique_id += 1
        data["id"] = self.unique_id
        data["url"] = response.url
        data["timestamp_crawl"] = datetime.now().strftime("%y-%m-%dT%H:%M:%S")
        data["name"] = response.css('div.parent h3 a::text').get()

        data["date_of_birth"] = response.xpath('//time[./a[contains(@href,"birth")]]/@datetime').get()
        data["place_of_birth"] = response.xpath('//a[contains(@href,"birth_place")]/text()').get()
        data["date_of_death"] = response.xpath('//time[./a[contains(@href,"death")]]/@datetime').get()
        data["place_of_death"] = response.xpath('//a[contains(@href,"death_place")]/text()').get()

        mini_bio = response.css('div.soda.odd p *::text').getall()
        data["mini_bio"] = ''.join(mini_bio)
        time.sleep(0.3)
        yield data