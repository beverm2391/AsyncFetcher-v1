This is my asynchronous fetcher with rate limiting. It's a work in progress - I'm primarily using it to fetch large amounts of stock data form polygon.io without pagination. If i need 20k results and the pagination limit is 1k, thats 20 separate, synchronous API calls - at 2s/call thats a minimum of 40s. Instead, I can make 20 asynchronous calls at once and get the data in 2s. Now scale that up to 800ish results per call (minute data for a day for one ticker) * 252 trading days a year * 10 years * 503 stocks in the S&P 500 = 1,267,560 API calls. With pagination (assuming 2s per call) that would amount to 704.2 hours. (This is assuming that all 503 stocks have been listed for 10 years, which is not true), but you get the idea of an IO bound process.

## Completed
- [X] fix rate limiter and get test passing
- [X] add options for parsing responses (whatever i need for polygon)
- [X] add tests for parsing responses
- [X] figure out whats wrong with  my code in `polygon_v1.ipynb` notebook - when passing in a list of 1 ticker it outputs 4 lists of urls - might be the list comp
- [X] get the fetcher working in notebook

## TODO 
- [ ] create some sort of schema for the polygon response
- [ ] move url generation logic to library, write unit tests
- [ ] fetch 5 years worth of minute data for the 93 stocks (07-01-2011 - 06-30-2021)
- [ ] use that data to replicate the results of the paper