import scrapy
from urllib.parse import urljoin
import pandas as pd
import time

class AmazonReviewsSpider(scrapy.Spider):
    name = "amazon_reviews"
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.scraped_reviews = set()
        self.comment_counter=0
        
    
    # List to store links with authentication errors
    auth_error_links = []
    scraped_review_ids = set()

    def start_requests(self):
        df = pd.read_excel('basic_scrapy_spider\AMZ Active ASIN list.xlsx', header=0)
        asin_list = df['ASIN'].tolist()
        for asin in asin_list:
            amazon_reviews_url = f'https://www.amazon.com/product-reviews/{asin}/'
            if self.comment_counter <= 50:
                yield scrapy.Request(url=amazon_reviews_url, callback=self.parse_reviews, meta={'asin': asin, 'retry_count': 0})
            else:
                self.comment_counter=0
                time.sleep(5)  # Adjust the sleep duration as needed

    def parse_reviews(self, response):
        asin = response.meta['asin']
        retry_count = response.meta['retry_count']
        

        # Get Next Page Url
        next_page_relative_url = response.css(".a-pagination .a-last>a::attr(href)").get()
        if next_page_relative_url is not None:
            retry_count = 0
            next_page = urljoin('https://www.amazon.com/', next_page_relative_url)

            
            yield scrapy.Request(url=next_page, callback=self.parse_reviews, meta={'asin': asin, 'retry_count': retry_count})
        
        # Adding this retry_count to bypass any amazon js rendered review pages
        elif retry_count < 3:
            retry_count = retry_count + 1
            yield scrapy.Request(url=response.url, callback=self.parse_reviews, dont_filter=True, meta={'asin': asin, 'retry_count': retry_count})

        # Parse Product Reviews
        review_elements = response.css("#cm_cr-review_list div.review")
        
        for review_element in review_elements:
         comment_text = "".join(review_element.css("::text").getall()).strip()
         comment_hash=hash(comment_text)
         if comment_hash not in self.scraped_reviews:
            self.scraped_reviews.add(comment_hash)
            self.comment_counter+=1
            yield {
                    "asin": asin,
                    "text": "".join(review_element.css("span[data-hook=review-body] ::text").getall()).strip(),
                    "title": review_element.css("*[data-hook=review-title]>span::text").get(),
                    "location_and_date": review_element.css("span[data-hook=review-date] ::text").get(),
                    "verified": bool(review_element.css("span[data-hook=avp-badge] ::text").get()),
                    "rating": review_element.css("*[data-hook*=review-star-rating] ::text").re(r"(\d+\.*\d*) out")[0],
                }

                # Add a sleep interval after a certain number of comments
        
