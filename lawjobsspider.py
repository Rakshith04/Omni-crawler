from urlparse import urljoin, urlparse
import re
from scrapy import Request
from scrapy.item import Item, Field
from scrapy.loader import ItemLoader
from scrapy.loader.processors import Identity
from scrapy.spiders.crawl import CrawlSpider, Rule
from scrapylib.processors import default_input_processor, default_output_processor
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.linkextractors import LinkExtractor
from scrapy.selector import Selector
from geopy.geocoders import Nominatim

__author__ = 'ttomlins'


class NormalizedJoin(object):
    """ Strips non-empty values and joins them with the given separator. """

    def __init__(self, separator=u' ', return_list=False):
        self.separator = separator
        self.return_list = return_list

    def __call__(self, values):
        result = self.separator.join(
            [value.strip() for value in values if value and not value.isspace()])
        if self.return_list:
            return [result]
        else:
            return result


class JobItem(Item):
    # required fields
    title = Field()
    # a unique id for the job on the crawled site.
    job_id = Field()
    # the url the job was crawled from
    url = Field()
    # name of the company where the job is.
    company = Field()

    # location of the job.
    # should ideally include city, state and country.
    # postal code if available.
    # does not need to include street information
    location = Field()
    description = Field()

    # the url users should be sent to for viewing the job. Sometimes
    # the "url" field requires a cookie to be set and this "apply_url" field will be differnt
    # since it requires no cookie or session state.
    apply_url = Field()

    # optional fields
    industry = Field()
    baseSalary = Field()
    benefits = Field()
    requirements = Field()
    skills = Field()
    work_hours = Field()


class JobItemLoader(ItemLoader):
    default_item_class = JobItem
    default_input_processor = default_input_processor
    default_output_processor = default_output_processor
    # all text fields are joined.
    description_in = Identity()
    description_out = NormalizedJoin()
    requirements_in = Identity()
    requirements_out = NormalizedJoin()
    skills_in = Identity()
    skills_out = NormalizedJoin()
    benefits_in = Identity()
    benefits_out = NormalizedJoin()


REF_REGEX = re.compile(r'\/(\d+)$')

APPEND_GB = lambda x: x.strip() + ", GB"

DOMAIN = "http://www.simplylawjobs.com"

QUERY_PARAM = "jobs?page="

class SimplyLawJobs(CrawlSpider):
    """ Should navigate to the start_url, paginate through
    the search results pages and visit each job listed.
    For every job details page found, should produce a JobItem
    with the relevant fields populated.

    You can use the Rule system for CrawSpider (the base class)
    or you can manually paginate in the "parse" method that is called
    after the first page of search results is loaded from the start_url.

    There are some utilities above like "NormalizedJoin" and JobItemLoader
    to help making generating clean item data easier.
    """
    
    name = 'lawjobsspider'
    allowed_domains = ["www.simplylawjobs.com"]
    start_urls = ["http://www.simplylawjobs.com/jobs",]

    def parse(self, response):

        sites = response.selector.xpath('//div[@id="pagination"]').extract()
        page_number = [i for i in re.findall(r'>(.*?)<',str(sites)) if i.isdigit()]
        page_number = map(int, page_number)

        #Change max(page_number) to specific number of pages you want to scrape 
        for page in xrange(min(page_number), max(page_number)+1):
            next_page = urljoin(DOMAIN, QUERY_PARAM+str(page))
            yield Request(next_page,callback=self.parseJobs)

    def parseJobs(self, response):

        for job_url in response.xpath('//div[@class="info font-size-small"]/a[1]/@href').extract():
            JobPage = urljoin(DOMAIN, str(job_url))
            yield Request(JobPage,callback=self.parseJobDetails)

    def parseJobDetails(self,data):

        l = JobItemLoader(item=JobItem(),response=data)

        #Get LOCATION  
        try:
            loc = data.selector.xpath('//*[@id="center_column"]/div[2]/div[2]/a[2]/text()').extract()
            geolocator = Nominatim(user_agent="simplylaw")
            location = geolocator.geocode(loc[0])
            a = location.address.split(',')
            b = str(loc[0])+','+str(a[-3])+','+str(a[-2])+','+str(a[-1])
            l.add_value('location', b)
        except Exception as e:
            l.add_xpath('location', '//*[@id="center_column"]/div[2]/div[2]/a[2]/text()')
        
        #Get DESCRIPTION
        if l.get_xpath('//div[@class="description allow-bulletpoints hide-for-small"]/p/text()'):
            l.add_xpath('description', '//div[@class="description allow-bulletpoints hide-for-small"]/p/text()')

        elif l.get_xpath('//div[@class="description allow-bulletpoints hide-for-small"]/p/span/text()'):
            l.add_xpath('description','//div[@class="description allow-bulletpoints hide-for-small"]/p/span/text()')

        elif l.get_xpath('//div[@class="description"]/text()'):
            l.add_xpath('description','//div[@class="description"]/text()')

        else:
            l.add_xpath('description', '//div[@class="description allow-bulletpoints hide-for-small"]/text()')

        #Get URL
        l.add_value('url', data.url)

        #Get TITLE
        l.add_xpath('title', '//h1[@class="job_title"]/text()')

        #Get COMAPNY NAME
        l.add_xpath('company', '//*[@id="center_column"]/div[2]/div[2]/a[1]/text()')
       
        #Add JOB ID
        job_id = str(data.url).split('/')[-1]
       
        if '?' in job_id:
            job_id = re.match(r'(\d+)',job_id)
            job_id = job_id.group()

        l.add_value('job_id',job_id)
        yield l.load_item()
