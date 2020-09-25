from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from datetime import datetime

class moviespider(CrawlSpider):
    name = "movie"
    unique_id = 0
    # seed url
    # type of comedy is limited to movie and show
    # genres is limited to comedy
    start_urls = ['https://www.imdb.com/search/title/?title_type=movie,show&genres=comedy&count=250&view=simple',]
	# limit the number of movie we retrive
    custom_settings = {'CLOSESPIDER_ITEMCOUNT': 5500}

    # next page
    rules = [Rule(LinkExtractor(restrict_css=('div.desc a')),follow=True,callback='parse_listpage'),]

    def parse_listpage(self,response):
    	# page that show the list of comedy movie

    	# links for each comedy movie
    	queues = response.css('span.lister-item-header a::attr(href)').getall()
    	for i in queues:
            yield response.follow(i,callback=self.parse_detail)


    def parse_detail(self, response):
    	# output detail
        data = {}
        self.unique_id += 1
        data["id"] = self.unique_id
        data["url"] = response.url
        data["timestamp_crawl"] = datetime.now().strftime("%y-%m-%dT%H:%M:%S")

        data["title"] = response.css('h1::text')[0].get().strip()
        data["genres"] = response.css('div.subtext a[href*="genres"]::text').getall()
        data["languages"] = response.xpath('//div[@class="txt-block"][contains(.//h4,"Language")]/a/text()').getall()
        release_date = response.xpath('//div[@class="txt-block"][contains(.//h4,"Release Date")]/text()').getall()
        if release_date == []:
            data["release_date"] = ''
        else:
            data["release_date"] = release_date[1].strip().split(" (")[0].lower()

        Budget = response.xpath('//div[@class="txt-block"][contains(.//h4,"Budget")]/text()').getall()
        if Budget == []:
            data["budget"] = ''
        else:
            data["budget"] = Budget[1].strip().split(" (")[0].lower()

        gross = response.xpath('//div[@class="txt-block"][contains(.//h4,"Cumulative Worldwide Gross")]/text()').getall()

        if gross == []:
            data["gross"] = ''
        else:
            data["gross"] = gross[1].strip()
        data['runtime'] = response.xpath("//div[contains(@class, 'txt-block')][contains(.//h4, 'Runtime')]/time/text()").get()

        yield data