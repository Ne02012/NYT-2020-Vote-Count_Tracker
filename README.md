# NYT Vote Scraper
Scrapes the NYT Votes Remaining Page JSON at https://static01.nyt.com/elections-assets/2020/data/api/2020-11-03/votes-remaining-page/national/president.json and commits it back to this repo. The goal is to be able to view history and diffs of `results.json`.
Also includes Wayback Machine scrapes of the above page before this auto scraper was deployed.

The TabulateVoteCounts.py script extracts the vote counts from the snapshots of the above page and also from the precinct-metadata files referenced by that and generates a table at VoteCounts.csv.

## Outputted files

- <https://alex.github.io/nyt-2020-election-scraper/battleground-state-changes.html>
- <https://github.com/alex/nyt-2020-election-scraper/blob/master/battleground-state-changes.txt>
- <https://alex.github.io/nyt-2020-election-scraper/battleground-state-changes.csv>
- <https://alex.github.io/nyt-2020-election-scraper/battleground-state-changes.xml>


## Inspired By
Simon Willison: <https://simonwillison.net/2020/Oct/9/git-scraping/>



## Development

Dependencies

* Python 3 is required


```
pip install -r requirements.txt
 ```

Contributions are welcome, but please make sure you read and fill out the [the pull request template](.github/pull_request_template.md) when submitting your changes. We would also appreciate it if you could read the short [contributing guide](https://github.com/alex/nyt-2020-election-scraper/blob/master/CONTRIBUTING.md).

Please do not modify any of the static files (html, csv, txt, or xml). These files are dynamically generated.

## To Support The Creators
We'd rather any money go to a good cause. Send any donations instead to <https://voting.works>!
