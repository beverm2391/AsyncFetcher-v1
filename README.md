## AcychFetcher-v1 AKA Polygon Mega Fetcher

This is my asynchronous fetcher with rate limiting (concurrency control). It's a work in progress - I'm primarily using it to fetch large amounts of stock data form polygon.io without pagination.

I needed to fetch 1256 days (5yrs) * 93 tickers of minute data in a reasonable amount of time to build a dataset. Using Polygon's pagination (more than 1000 results returns a link with the next page) creates an I/O bottleneck. 

Say each request takes ~1s to return with the link for the next request, and assuming ~800 results a day, we'll round up and estimate 1 request per day, per stock. That amounts to 116,808 sychronous API calls @ ~1s per call = ~32.45 hours. 

The natural solution is to make batches of smaller calls (less than the pagination limit of 1k) asychronously. That's why I built this fetcher, which sends batch GET requests while handling errors with exponential backoff and adhering to a global concurrency limit. 

For a single stock (AAPL), I fetched, validated, parsed, and saved 5 years of minute date (~480k rows) in 10.08s! Nice!
I ended up getting all 93 stocks in under an hour. Since each stock had about 480k rows, my total dataset was about 44.6 million rows. 

## Completed
- [X] fix rate limiter and get test passing
- [X] add options for parsing responses (whatever i need for polygon)
- [X] add tests for parsing responses
- [X] figure out whats wrong with  my code in `polygon_v1.ipynb` notebook - when passing in a list of 1 ticker it outputs 4 lists of urls - might be the list comp
- [X] get the fetcher working in notebook
- [X] create a Pydantic schema for the polygon response
- [X] move url generation logic to library
- [X] finish test_utils
- [X] finish test_polygon
- [X] fix tuple generator to use only date string or unix timestamp
- [X] write fetcher script from `polygon_v1.ipynb` notebook
  - [X] sanity check the data
- [X] do some *light* stress testing of polygon API??
- [X] make new script to do one ticker at a time, aggregate, and write to csv, so that if something breaks I don't lose everything 
- [X] fetch 5 years worth of minute data for the 93 stocks (07-01-2011 - 06-30-2021)

## TODO 
- [ ] get repo ready for other people to use
  - [ ] add requirements.txt
  - [ ] figure out the best convention for python packaging for community ease of use and package maintenance
    - [ ] find a good package to copy/use as reference
  - [ ] add setup.py (figure out how to install the package)
  - [ ] add docs and example scripts
  - [ ] get friends (leafboats) to test out the package/workflow
- [ ] add tqdm or something to show progress and overall rate metrics?
- [ ] improve the fetcher where you can cancel a run and still have the data you've fetched so far (like a cache?)