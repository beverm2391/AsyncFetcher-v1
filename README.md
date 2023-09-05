I built this so that I could fetch multiple years of intraday data from [polygon](https://polygon.io/) from all 503 stocks in the S&P 500.

Since polygon:
 - limits the results per request
 - paginates the results that it does return

Since this task is ultimately I/O bound, I figured the fastest way would be to avoid any paginated results and instead send a bunch of async requests.

I ended up making 1 request per stock per day, and then aggregating the results into a single CSV file per stock.

For 503 stocks (S&P 500) over 5 years (~252 * 5 = 1260 trading days), this ended up being 503 * 1260 = ~634k requests.

I needed a way to handle errors, missing data, rate limits, and any other issues that might arise. Thus I made this fetcher. 

This repo only includes the code for the fetcher itself, not the code used to generate the req urls or the code used to aggregate the results.

This code is not perfect - but it did the job for me.