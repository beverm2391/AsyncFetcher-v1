This is my asynchronous fetcher with rate limiting. It's a work in progress - I'm primarily using it to fetch large amounts of stock data form polygon.io without pagination. If i need 20k results and the pagination limit is 1k, thats 20 separate, synchronous API calls - at 2s/call thats a minimum of 40s. Instead, I can make 20 asynchronous calls at once and get the data in 2s. Now scale that up to 800ish results per call (minute data for a day for one ticker) * 252 trading days a year * 10 years * 503 stocks in the S&P 500 = 1,267,560 API calls. With pagination (assuming 2s per call) that would amount to 704.2 hours. (This is assuming that all 503 stocks have been listed for 10 years, which is not true), but you get the idea of an IO bound process.

Did a successful run with AAPL. Fetched, validated, parsed, and saved 5 years of minute date (~500k rows) in 10.08s! Nice!

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
- 
## TODO 
- [ ] get repo ready for other people to use
  - [ ] add requirements.txt
  - [ ] add setup.py (figure out how to install the package)
  - [ ] add docs and example scripts
- [ ] add tqdm or something to show progress and overall rate metrics?
- [ ] improve the fetcher where you can cancel a run and still have the data you've fetched so far (like a cache?)